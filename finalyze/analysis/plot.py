from pathlib import Path

from finalyze.config import config

TEMPLATE_DIR = Path(__file__).parent.parent.joinpath("templates").resolve()
TABLE_PAGE = (TEMPLATE_DIR / "table.html").read_text()
PLOTS_PAGE = (TEMPLATE_DIR / "plots.html").read_text()
INDEX_PAGE = (TEMPLATE_DIR / "index.html").read_text()
CSS = (TEMPLATE_DIR / "style.css").read_text()
FIGURE_DIV = (TEMPLATE_DIR / "figure.html").read_text()
INDEX_SCRIPT = (TEMPLATE_DIR / "index_script.js").read_text()
FIGURE_SCRIPT = (TEMPLATE_DIR / "figures_script.js").read_text()


def plots_html(tables, title):
    color_map = {
        name: color.as_hex() for name, color in config().analysis.graphs.colors.items()
    }
    lightweight = config().analysis.graphs.lightweight_html
    template = config().analysis.graphs.plotly_template
    divs = []
    plotly_js_included = False
    for i, table in enumerate(tables):
        if table.has_figure:
            fig = table.get_figure(
                template=template,
                color_discrete_map=color_map,
                **config().analysis.graphs.plotly_arguments,
            )
            if not fig:
                continue
            include_plotlyjs = False
            if not plotly_js_included:
                include_plotlyjs = "cdn" if lightweight else True
            plotly_js_included = True
            fig_html = fig.to_html(full_html=False, include_plotlyjs=include_plotlyjs)
            div = FIGURE_DIV.format(figure=fig_html, plot_title=table.title)
            divs.append(div)
        else:
            formatted_table = format_table_html(table.source)
            div = f'<div class="box table-box">\n<h2>{table.title}</h2>\n<div>\n{formatted_table}\n</div>\n</div>'
            divs.append(div)
    all_figures = "\n".join(divs)
    return PLOTS_PAGE.format(
        css=CSS,
        title=title,
        figures=all_figures,
        script=FIGURE_SCRIPT,
    )


def format_table_html(table):
    formatted_rows = ["".join(f"<th>{v}</th>" for v in table.columns)]
    for row in table.iter_rows():
        formatted_rows.append("".join(f"<td>{v}</td>" for v in row))
    formatted_rows = "".join(f"<tr>{r}</tr>" for r in formatted_rows)
    return f"<table>{formatted_rows}</table>"


def table_html(table, title):
    formatted_table = format_table_html(table)
    return TABLE_PAGE.format(
        css=CSS,
        title=title,
        table=formatted_table,
    )


def index_html(links, title):
    link_keys = {}
    named_links = {}
    for name, link in links.items():
        keys = list(link_keys.values())
        available_keys = [c for c in name.lower() if c not in keys]
        if not available_keys:
            available_keys = [str(i) for i in range(1, 10) if str(i) not in keys]
            if not available_keys:
                continue
        key = available_keys[0]
        link_keys[key] = link
        named_links[f"[{key}] {name}"] = link

    formatted_links = "\n<br>".join(
        f"<a href={link}>{name}</a>" for name, link in named_links.items()
    )
    script_links = ",\n".join(f"'{key}': '{link}'" for key, link in link_keys.items())
    script = INDEX_SCRIPT.format(links=script_links)
    return INDEX_PAGE.format(
        css=CSS,
        title=title,
        links=formatted_links,
        script=script,
    )
