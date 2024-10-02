import matplotlib.colors as mcolors
import numpy as np
import plotly.graph_objects as go


class VizEngine:
    def __init__(self):
        pass

    def __get_color_from_range(
        self,
        input,
        min_input=-0.15,
        max_input=0.15,
        min_color="red",
        mid_color="yellow",
        max_color="green",
    ):
        """
        Generate a color given an input variable, input variable range, and color min/mid/max range

        Parameters
        ----------
        input : float
        min_input : float
        max_input : float
        min_color : str or rgb
        mid_color : str or rgb
        max_color : str or rgb

        Returns
        -------
        rgb
        """
        # Create a continuous color scale between red, yellow, and green
        cmap = mcolors.LinearSegmentedColormap.from_list(
            "red_yellow_green", [min_color, mid_color, max_color]
        )
        norm_input = np.clip((input - min_input) / (max_input - min_input), 0, 1)
        color = cmap(norm_input)
        return f"rgba({int(color[0] * 255)}, {int(color[1] * 255)}, {int(color[2] * 255)}, 0.5)"

    def create_best_bets_graph(self, best_bets, year, week, market_type):
        if market_type == "spreads":
            market_val = "away_spread"
            model_val = "away_spread_modeled"
            x_range = [14, -14]
            x_title = "Away Team Point Spread"
        elif market_type == "h2h":
            market_val = "price"
            model_val = "break_even_odds"
            x_range = [500, -500]
            x_title = "Moneyline Price"
        else:
            raise Exception(
                f"Invalid market type {market_type}, expected h2h or spreads"
            )
        fig = go.Figure()
        df = best_bets.sort_values(by=["expected_value"], ascending=True).reset_index(
            drop=True
        )
        for i, game in df.iterrows():
            y = i
            fig.add_trace(
                go.Scatter(
                    x=[
                        game[market_val],
                        game[model_val],
                        game[model_val],
                        game[market_val],
                    ],
                    y=[y - 0.15, y - 0.15, y + 0.15, y + 0.15],
                    fill="toself",
                    fillcolor=self.__get_color_from_range(
                        game["expected_value"]
                    ),  # Use the color mapping function
                    line=dict(color="rgba(255,255,255,0)"),  # No visible boundary
                    name=f'EV: {np.round(game["expected_value"]*100, 1)}%',
                    hovertemplate=f'EV: {game["expected_value"]*100:.1f}%<extra></extra>',
                    hoverinfo="text",
                )
            )

            color = "green" if game["expected_value"] > 0 else "red"
            fig.add_trace(
                go.Scatter(
                    x=[game[model_val], game[model_val]],
                    y=[y - 0.45, y + 0.45],
                    mode="lines",
                    line=dict(color=color, width=4),
                    name=f"Modeled: {np.round(game[model_val], 1)}",  # Legend name (if shown)
                    hovertemplate=f"Modeled: {np.round(game[model_val], 1)}<extra></extra>",  # Hover info
                    hoverinfo="text",
                    text=[f"Modeled: {np.round(game[model_val], 1)}"] * 2,
                    textposition="middle right",
                )
            )

            fig.add_trace(
                go.Scatter(
                    x=[game[market_val], game[market_val]],
                    y=[y - 0.45, y + 0.45],
                    mode="lines",
                    line=dict(color="black", width=4),
                    name=f"Vegas: {game[market_val]}",
                    hovertemplate=f"Vegas: {game[market_val]}<extra></extra>",
                    hoverinfo="text",
                    text=[f"Vegas: {np.round(game[market_val], 1)}"] * 2,
                    textposition="middle left",
                )
            )

        ticktext = []
        bet_rec = (
            f'bet {np.round(game["ideal_bet_pcnt_diluted"]*100, 1)}% @{game["book"]}'
        )
        for _, game in df.iterrows():
            bet_rec = f'bet {np.round(game["ideal_bet_pcnt_diluted"]*100, 1)}% @{game["book"]}'
            sign = ""
            if game["away_spread"] > 0:
                sign = "+"
            if game["outcome"] == game["home"]:
                ticktext.append(
                    f"{game['away']} {sign}{game['away_spread']} @<b>{game['home']}</b> {bet_rec}"
                )
            else:
                ticktext.append(
                    f"<b>{game['away']}</b> {sign}{game['away_spread']} @{game['home']} {bet_rec}"
                )

        fig.update_layout(
            xaxis=dict(range=x_range, title=x_title),
            yaxis=dict(tickvals=np.arange(len(df)), ticktext=ticktext, title="Teams"),
            title=f"Best Bets {market_type}",
            showlegend=False,
        )

        fig.write_html(
            f"../output/visualizations/predictions/{year}_w{week}_{market_type}_value.html"
        )
        fig.show()
        return fig
