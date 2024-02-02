import os

import dotenv
import pandas as pd
import pyppeteer
from bs4 import BeautifulSoup

import config
import src.helpers.set_timestamp as set_timestamp

dotenv.load_dotenv()
base_url = "https://data.vsin.com/{}/betting-splits/"
sport_list = [
    # "college-basketball",
    "college-football",
    "nfl",
    "nba",
    "mlb",
    # "golf/pga",
]
api_key = os.environ.get("BROWSERLESS_API_KEY")
vsin_user = os.environ.get("VSIN_USER")
vsin_pw = os.environ.get("VSIN_PW")
wait_time = config.PAGE_LOAD_DELAY_MS


async def __find_nested_frame_with_element(frame, element_str):
    # Print the URL of the current frame
    print(frame.url)
    ele = await frame.querySelector(element_str)
    if ele:
        return frame

    # Check if there are child frames within this frame
    child_frames = frame.childFrames
    if child_frames:
        # If there are child frames, recursively process each of them
        for child_frame in child_frames:
            child_child_frame = await __find_nested_frame_with_element(
                child_frame, element_str
            )
            if child_child_frame:
                return child_child_frame
    return None


async def __find_frame_with_element(page, element_name):
    for frame in page.frames:
        frame_with_element = await __find_nested_frame_with_element(frame, element_name)
        if frame_with_element:
            break
    return frame_with_element


async def __click_login_link(page):
    print("clicking login link")
    link_frame = await __find_frame_with_element(page, element_name=".main__link")
    button = await link_frame.waitForSelector(".main__link")
    if button:
        await button.click()
    else:
        print("no button found")


async def __login(page):
    print("logging in")
    login_frame = await __find_frame_with_element(page, element_name='[name="email"]')
    if login_frame:
        password_input = await login_frame.waitForSelector("[fieldloginpassword]")
        email_input = await login_frame.querySelector('[name="email"]')

        # Fill in the email and password fields
        await email_input.type(vsin_user)
        await password_input.type(vsin_pw)

        login_button = await login_frame.waitForXPath(
            '//button[@actionlogin]/span/t[contains(text(), "Login")]'
        )
        await login_button.click()
    else:
        print("no login frame found")


async def __close_login_success(page):
    print("exiting login success window")
    exit_frame = await __find_frame_with_element(page, ".close-button-wrapper")

    exit_button = await exit_frame.waitForSelector(".close-button-wrapper")

    if exit_button:
        await exit_button.click()
    else:
        print("no wizard found")


def __get_tables_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    return tables


def __get_col_titles(headers):
    col_names = []
    for cell in headers:
        for td in cell:
            col_names.append(td.text)
    game_date = col_names[0].replace("\xa0", "")
    home_cols = ["home_team"] + [f"home_{name}" for name in col_names[1:]]
    away_cols = ["away_team"] + [f"away_{name}" for name in col_names[1:]]
    full_col_names = ["game_date"] + away_cols + home_cols
    return full_col_names, game_date


def __rename_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in df.columns[df.columns.duplicated(keep=False)]:
        cols[df.columns.get_loc(dup)] = [
            dup + "_" + str(d_idx) if d_idx != 0 else dup
            for d_idx in range(sum(df.columns == dup))
        ]
    df.columns = cols
    return df


def __element_filter(tag):
    return (
        tag.get("role") != "button"
        and "txt-color-vsinred" in tag.get("class", [])
        and not tag.get("title", "").strip() == "Bet Now at DraftKings"
    ) or (
        tag.name == "div"
        and (
            "text-center" in tag.get("class", [])
            and "fw-bold" in tag.get("class", [])
            and "box_highlight_for" not in tag.get("class", [])
            and "box_highlight_agn" not in tag.get("class", [])
        )
        or ("scorebox_highlight" in tag.get("class", []))
    )


def __vsin_table_to_df(table):
    full_data = []
    rows = table.find_all("tr")
    game_date = "No Date Found"

    if rows.__len__() > 1:
        for row_i, row in enumerate(rows):
            # check if it's the date row
            if "Handle" in row.text:
                headers = row.find_all("th")
                full_col_names, game_date = __get_col_titles(headers)
            else:
                away_data = []
                home_data = []
                # check that it's not the header row
                if "Betting Splits" not in row.text:
                    cells = row.find_all("td")
                    for cell in cells:
                        good_data = cell.find_all(__element_filter)
                        for i, datum in enumerate(good_data):
                            for string in datum.stripped_strings:
                                if i % 2 == 0:
                                    away_data.append(string)
                                else:
                                    home_data.append(string)
                    full_data.append([game_date] + away_data + home_data)
        df = pd.DataFrame(data=full_data, columns=full_col_names)
        df = __rename_duplicate_columns(df)
        set_timestamp.set_timestamp()
        df["timestamp"] = config.TIMESTAMP
    else:
        df = pd.DataFrame()
    return df


async def __login_and_clear_windows(browser):
    page = await browser.newPage()
    # TODO this isn't waiting correctly. Frame name?
    url = base_url.format(sport_list[0])
    await page.goto(url)
    await page.waitFor(wait_time)
    await __click_login_link(page)
    # TODO do a better wait
    await page.waitFor(wait_time)

    await __login(page)

    await page.waitFor(wait_time)

    await __close_login_success(page)
    await page.waitFor(wait_time)
    return page


async def __get_vsin_game_lines_one_sport(page, sport_name):
    url = "https://data.vsin.com/{}/betting-splits/".format(sport_name)
    await page.goto(url)
    await page.waitFor(wait_time)

    html = await page.content()
    tables = __get_tables_from_html(html)
    # TODO add loop for multiple tables
    df = __vsin_table_to_df(tables[0])
    if not df.empty:
        df["sport"] = sport_name.replace("/", "-")
    return df


async def get_vsin_game_lines():
    browser = await pyppeteer.launch(headless=False)
    page = await __login_and_clear_windows(browser)
    df_list = []
    for sport in sport_list:
        df = await __get_vsin_game_lines_one_sport(page, sport_name=sport)
        if not df.empty:
            df.to_csv(
                f"../output/game_lines/{config.TIMESTAMP}_{sport}_lines.csv",
                index=False,
            )
            df_list.append(df)
    df = pd.concat(df_list)
    df.to_csv("../output/game_lines/all_lines.csv")
    await browser.close()
    return df
