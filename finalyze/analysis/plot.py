from pathlib import Path

TEMPLATE_DIR = Path(__file__).parent.parent.joinpath("templates").resolve()
HTML = (TEMPLATE_DIR / "index.html").read_text()
CSS = (TEMPLATE_DIR / "style.css").read_text()
SCRIPT = (TEMPLATE_DIR / "script.js").read_text()
FIGURE_DIV = (TEMPLATE_DIR / "figure.html").read_text()


def write_html(source_table, tables, config):
    color_map = {
        name: color.as_hex() for name, color in config.analysis.graphs.colors.items()
    }
    lightweight = config.analysis.graphs.lightweight_html
    template = config.analysis.graphs.plotly_template
    title = "Plots"
    # Plots
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
    # Filters
    formatted_filters_lines = []
    field_names = config.analysis.filters.__class__.model_fields
    for field_name in field_names:
        field_value = getattr(config.analysis.filters, field_name)
        if field_value is None:
            continue
        formatted_filters_lines.append(f"<b>{field_name}:</b> {field_value}")
    formatted_filters_lines = formatted_filters_lines or ["No filters."]
    formatted_filters = "<br>".join(formatted_filters_lines)
    # Source data table
    formatted_source_rows = ["".join(f"<th>{v}</th>" for v in source_table.columns)]
    for row in source_table.iter_rows():
        formatted_source_rows.append("".join(f"<td>{v}</td>" for v in row))
    formatted_source_rows = "".join(f"<tr>{r}</tr>" for r in formatted_source_rows)
    formatted_source_table = f"<table>{formatted_source_rows}</table>"
    # Generate and export html
    html = HTML.format(
        css=CSS,
        script=SCRIPT,
        title=title,
        main_content=content,
        filters=formatted_filters,
        source_table=formatted_source_table,
    )
    Path(config.general.plots_file).write_text(html)
