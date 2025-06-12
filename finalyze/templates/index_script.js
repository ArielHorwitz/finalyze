document.addEventListener('keydown', e => {{
    const links = {{ {links} }};
    if (links[e.key]) {{
        window.location.href = links[e.key]
    }}
}});
