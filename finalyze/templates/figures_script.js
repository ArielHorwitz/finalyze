const boxes = document.querySelectorAll('.figure-box');
let currentIndex = 0;

document.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowRight' && currentIndex < boxes.length - 1) {
        currentIndex++;
        boxes[currentIndex].scrollIntoView({ behavior: 'smooth' });
    } else if (e.key === 'ArrowLeft' && currentIndex > 0) {
        currentIndex--;
        boxes[currentIndex].scrollIntoView({ behavior: 'smooth' });
    }
});
