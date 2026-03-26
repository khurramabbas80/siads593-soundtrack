import altair as alt

# Standardized chart dimensions that should look good in a report
DEFAULT_WIDTH = 560
DEFAULT_HEIGHT = 320

def team_theme():
    """
    Team-wide Altair theme:
    - Consistent fonts (Lato), axis/grid styling, legend styling
    - Simple, readable defaults that paste cleanly into a Google Doc
    """
    return {
        "config": {
            # Optional neutral background inspired by the Elegant Wedding palette:
            # Use only when a soft, editorial feel is desired.
            # Disabled by default for report figures, since white backgrounds
            # reproduce more reliably in Google Docs and PDFs.
            "background": "white",
            "view": {
                "fill": "white",
                "stroke": "transparent"   # removes the plot-area border for Tufte-minimalism
            },

            # Typography (Lato + fallbacks if Lato isn't available)
            "font": "Lato",

            # Style for the main chart title text (only shows up if you set .properties(title="..."))
            "title": {
                "font": "Lato",
                "fontSize": 16,
                "fontWeight": 600,
                "color": "#111111",            # main title color
                "subtitleColor": "#5E17A6",   # subtitle color
                "anchor": "middle", # "start" left-aligns titles for a report-style look
            },
            # Axis defaults: readable labels, light gridlines, subtle axis/tick styling.
            "axis": {
                "labelFont": "Lato",
                "titleFont": "Lato",
                "labelFontSize": 12,
                "titleFontSize": 12,

                # Tufte-minimalist settings
                "grid": False,      # no gridlines
                "ticks": False,     # no tick marks
                "domain": False,    # no axis baseline

                # Leave the colors on in case we turn things back on
                "gridColor": "#e9e9e9",
                "tickColor": "#cccccc",
                "domainColor": "#cccccc",
            },

            # Legend typography to match axes (keeps multi-chart docs feeling cohesive).
            "legend": {
                "labelFont": "Lato",
                "titleFont": "Lato",
                "labelFontSize": 12,
                "titleFontSize": 12,
            },

            # Mark defaults: slightly larger points + thicker lines for readability in docs.
            "point": {"filled": True, "size": 60},
            "line": {"strokeWidth": 2},

            # Categorical color palette inspired by the "Elegant Wedding" palette:
            # https://www.color-hex.com/color-palette/1054967
            # Note: the original palette includes a very light beige (#f5f5dc),
            # which we intentionally exclude here to avoid low-contrast series colors
            # on white report backgrounds.
            "range": {
                "category": [
                    "#7922CC", "#1195B2", "#CC0000", "#CE7E00",
                    "#5E17A6", "#0E7C93", "#9E0000", "#A86600",
                    "#3F1D5C", "#1F6F5B", "#8C4A00"
                ]
            }
        }
    }

def enable():
    """
    Register + enable the theme for the current Python session.
    Call this once near the top of each notebook/script before creating charts.
    """
    alt.themes.register("team_theme", team_theme)
    alt.themes.enable("team_theme")

def sized(chart, width = DEFAULT_WIDTH, height = DEFAULT_HEIGHT):
    """
    Optional helper to keep chart dimensions consistent without repeating properties().
    Usage: sized(alt.Chart(df)...)
    """
    return chart.properties(width=width, height=height)