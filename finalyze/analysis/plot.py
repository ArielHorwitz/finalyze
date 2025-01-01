from pathlib import Path

TEMPLATE_DIR = Path(__file__).parent.parent.joinpath("templates").resolve()
HTML = (TEMPLATE_DIR / "index.html").read_text()
CSS = (TEMPLATE_DIR / "style.css").read_text()
FIGURE_DIV = (TEMPLATE_DIR / "figure.html").read_text()


def write_html(tables, config):
    color_map = {
        name: color.as_hex() for name, color in config.analysis.graphs.colors.items()
    }
    lightweight = config.analysis.graphs.lightweight_html
    template = config.analysis.graphs.plotly_template
    title = "Plots"
    divs = []
    for i, table in enumerate(tables):
        fig = table.get_figure(template=template, color_discrete_map=color_map)
        if not fig:
            continue
        is_first = i == 0
        include_plotlyjs = "cdn" if (is_first and lightweight) else is_first
        fig_html = fig.to_html(full_html=False, include_plotlyjs=include_plotlyjs)
        div = FIGURE_DIV.format(figure=fig_html, plot_title=table.title)
        divs.append(div)
    content = "\n".join(divs)
    html = HTML.format(css=CSS, title=title, main_content=content)
    Path(config.general.plots_file).write_text(html)
