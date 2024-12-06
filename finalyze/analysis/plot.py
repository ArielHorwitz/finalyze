from pathlib import Path

TEMPLATE_DIR = Path("finalyze/templates")
HTML = (TEMPLATE_DIR / "index.html").read_text()
CSS = (TEMPLATE_DIR / "style.css").read_text()
FIGURE_DIV = (TEMPLATE_DIR / "figure.html").read_text()


def write_html(
    tables,
    output_file,
    *,
    title="Plots",
    light=False,
    template="plotly_dark",
    color_map=None,
):
    include_plotlyjs = "cdn" if light else True
    divs = []
    for i, table in enumerate(tables):
        fig = table.get_figure(template=template, color_discrete_map=color_map)
        if not fig:
            continue
        fig_html = fig.to_html(
            full_html=False,
            include_plotlyjs=include_plotlyjs if i == 0 else False,
        )
        div = FIGURE_DIV.format(figure=fig_html, plot_title=table.title)
        divs.append(div)
    content = "\n".join(divs)
    html = HTML.format(css=CSS, title=title, main_content=content)
    output_file.write_text(html)
