from pathlib import Path

from finalyze.config import config

TEMPLATE_DIR = Path(__file__).parent.parent.joinpath("templates").resolve()
TABLE_PAGE = (TEMPLATE_DIR / "table.html").read_text()
PLOTS_PAGE = (TEMPLATE_DIR / "plots.html").read_text()
INDEX_PAGE = (TEMPLATE_DIR / "index.html").read_text()
CSS = (TEMPLATE_DIR / "style.css").read_text()
FIGURE_DIV = (TEMPLATE_DIR / "figure.html").read_text()


def plots_html(tables, title):
    color_map = {
        name: color.as_hex() for name, color in config().analysis.graphs.colors.items()
    }
    lightweight = config().analysis.graphs.lightweight_html
    template = config().analysis.graphs.plotly_template
    title = config().analysis.graphs.title
    # Plots
    divs = []
    for i, table in enumerate(tables):
        fig = table.get_figure(
            template=template,
            color_discrete_map=color_map,
            **config().analysis.graphs.plotly_arguments,
        )
        if not fig:
            continue
        is_first = i == 0
        include_plotlyjs = "cdn" if (is_first and lightweight) else is_first
        fig_html = fig.to_html(full_html=False, include_plotlyjs=include_plotlyjs)
        div = FIGURE_DIV.format(figure=fig_html, plot_title=table.title)
        divs.append(div)
    all_figures = "\n".join(divs)
    # Generate and export html
    return PLOTS_PAGE.format(
        css=CSS,
        title=title,
        figures=all_figures,
    )


def table_html(table, title):
    formatted_rows = ["".join(f"<th>{v}</th>" for v in table.columns)]
    for row in table.iter_rows():
        formatted_rows.append("".join(f"<td>{v}</td>" for v in row))
    formatted_rows = "".join(f"<tr>{r}</tr>" for r in formatted_rows)
    formatted_table = f"<table>{formatted_rows}</table>"
    return TABLE_PAGE.format(
        css=CSS,
        title=title,
        table=formatted_table,
    )


def index_html(links, title):
    formatted_links = "<br>".join(
        f"<a href={link}>{name}</li>" for name, link in links.items()
    )
    return INDEX_PAGE.format(
        css=CSS,
        title=title,
        links=formatted_links,
    )
