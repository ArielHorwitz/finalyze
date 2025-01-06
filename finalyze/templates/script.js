document.querySelector('.collapse-all').onclick = function() {
    const current_display = document.querySelectorAll('.collapsible-content')[0].style.display;
    document.querySelectorAll('.collapsible-content').forEach(content => {
        content.style.display = current_display === 'none' ? 'block' : 'none';
    });
};

document.querySelectorAll('.collapsible').forEach((collapsible, index) => {
    collapsible.onclick = function() {
        const content = document.querySelectorAll('.collapsible-content')[index];
        content.style.display = content.style.display === 'none' ? 'block' : 'none';
    };
});
