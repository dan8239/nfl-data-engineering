import matplotlib.colors as mcolors
import numpy as np
import plotly.graph_objects as go


class VizEngine:
    def __init__(self, best_bets):
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

    def create_best_bets_graph(self, best_bets, year, week):
        fig = go.Figure()
        df = best_bets.sort_values(by=["expected_value"], ascending=True).reset_index(
            drop=True
        )
        for i, game in df.iterrows():
            y = i
            fig.add_trace(
                go.Scatter(
                    x=[
                        game["away_spread"],
                        game["away_spread_modeled"],
                        game["away_spread_modeled"],
                        game["away_spread"],
                    ],
                    y=[y - 0.1, y - 0.1, y + 0.1, y + 0.1],
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
                    x=[game["away_spread_modeled"], game["away_spread_modeled"]],
                    y=[y - 0.45, y + 0.45],
                    mode="lines",
                    line=dict(color=color, width=4),
                    name=f'Modeled: {np.round(game["away_spread_modeled"], 1)}',  # Legend name (if shown)
                    hovertemplate=f'Modeled: {np.round(game["away_spread_modeled"], 1)}<extra></extra>',  # Hover info
                    hoverinfo="text",
                )
            )

            fig.add_trace(
                go.Scatter(
                    x=[game["away_spread"], game["away_spread"]],
                    y=[y - 0.45, y + 0.45],
                    mode="lines",
                    line=dict(color="black", width=4),
                    name=f'Vegas: {game["away_spread"]}',  # Legend name (if shown)
                    hovertemplate=f'Vegas: {game["away_spread"]}<extra></extra>',  # Hover info
                    hoverinfo="text",
                )
            )

            fig.add_annotation(
                x=14,
                y=y,
                text=f'{np.round(game["ideal_bet_pcnt_diluted"]*100, 1)}% {game["outcome"]} @{game["book"]}',  # Add outcome and bet percentage
                showarrow=False,
                font=dict(
                    size=12,
                    color=self.__get_co__get_color_from_range(game["expected_value"]),
                ),  # EV-based color
                xanchor="left",
                yanchor="middle",
            )

        fig.update_layout(
            xaxis=dict(range=[14, -14], title="Away Team Point Spread"),
            yaxis=dict(tickvals=np.arange(len(df)), ticktext=df["game"], title="Teams"),
            title="NFL Best Bets: Spreads",
            showlegend=False,
        )

        fig.write_html(
            f"../output/visualizations/predictions/{year}_{week}_spread_value.html"
        )
        fig.show()
