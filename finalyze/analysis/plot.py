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
    formatted_filters_lines = []
    field_names = config.analysis.filters.__class__.model_fields
    for field_name in field_names:
        field_value = getattr(config.analysis.filters, field_name)
        if field_value is None:
            continue
        formatted_filters_lines.append(f"<b>{field_name}:</b> {field_value}")
    formatted_filters_lines = formatted_filters_lines or ["No filters."]
    formatted_filters = "<br>".join(formatted_filters_lines)
    html = HTML.format(
        css=CSS,
        title=title,
        main_content=content,
        filters=formatted_filters,
    )
    Path(config.general.plots_file).write_text(html)
