import asyncio
import requests
import pyppeteer
import dotenv
import os
import pandas as pd
from bs4 import BeautifulSoup
import config
import set_timestamp

dotenv.load_dotenv()
url = "https://data.vsin.com/college-basketball/betting-splits/"
api_key = os.environ.get("BROWSERLESS_API_KEY")
vsin_user = os.environ.get("VSIN_USER")
vsin_pw = os.environ.get("VSIN_PW")


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


async def find_frame_with_element(page, element_name):
    for frame in page.frames:
        frame_with_element = await __find_nested_frame_with_element(frame, element_name)
        if frame_with_element:
            break
    return frame_with_element


async def click_login_link(page):
    print("clicking login link")
    link_frame = await find_frame_with_element(page, element_name=".main__link")
    button = await link_frame.waitForSelector(".main__link")
    if button:
        await button.click()
    else:
        print("no button found")


async def login(page):
    print("logging in")
    login_frame = await find_frame_with_element(page, element_name='[name="email"]')
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


async def close_login_success(page):
    print("exiting login success window")
    exit_frame = await find_frame_with_element(page, ".close-button-wrapper")

    exit_button = await exit_frame.waitForSelector(".close-button-wrapper")

    if exit_button:
        await exit_button.click()
    else:
        print("no wizard found")


async def exit_wizard(page):
    print("exiting wizard")
    exit_frame = await find_frame_with_element(
        page, 'button[aria-label="Close"].tp-close.tp-active'
    )

    exit_button = await exit_frame.waitForSelector(
        'button[aria-label="Close"].tp-close.tp-active'
    )

    if exit_button:
        await exit_button.click()
    else:
        print("no wizard found")


def get_tables_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    return tables


def vsin_table_to_df(table):
    headers = table.find("thead")
    col_names = []
    for child in headers.children:
        for td in child:
            col_names.append(td.text)
    game_date = col_names[1].replace("\xa0", "")
    print(game_date)
    col_names = [name for name in col_names if "\xa0" not in name]
    home_cols = ["home_team"] + [f"home_{name}" for name in col_names]
    away_cols = ["away_team"] + [f"away_{name}" for name in col_names]
    full_col_names = away_cols + home_cols

    away_data = []
    home_data = []
    full_data = []
    rows = table.find_all("tr")

    for row_i, row in enumerate(rows):
        if row_i > 1:
            cells = row.find_all("td")
            for cell in cells:
                for i, string in enumerate(cell.stripped_strings):
                    if i % 2 == 0:
                        away_data.append(string)
                    else:
                        home_data.append(string)
            full_data.append(away_data + home_data)
    df = pd.DataFrame(data=full_data, columns=full_col_names)
    df["game_date"] = game_date
    set_timestamp.set_timestamp()
    df["timestamp"] = config.TIMESTAMP
    return df


async def get_vsin():
    # browser = await pyppeteer.launcher.connect(
    #     browserWSEndpoint=f"wss://chrome.browserless.io?token={api_key}", headless=False
    # )
    browser = await pyppeteer.launch(headless=False)
    page = await browser.newPage()
    # await page.goto(url)
    # await page.waitForNavigation({"waitUntil": "load"})
    # TODO this isn't waiting correctly. Frame name?
    await page.goto(url)
    await page.waitFor(3000)
    await click_login_link(page)
    # TODO do a better wait
    await page.waitFor(3000)

    await login(page)

    await page.waitFor(3000)

    await close_login_success(page)
    await page.waitFor(3000)

    await page.goto(url)
    await page.waitFor(3000)

    html = await page.content()
    tables = get_tables_from_html(html)
    # TODO add loop for multiple tables
    df = vsin_table_to_df(tables[0])
    set_timestamp.set_timestamp()
    sport = "CBK"
    df.to_csv(f"../output/game_lines/{config.TIMESTAMP}_{sport}_lines.csv", index=False)
    await browser.close()


def handler(event, context):
    asyncio.get_event_loop().run_until_complete(get_vsin())


if __name__ == "__main__":
    handler(event=None, context=None)
