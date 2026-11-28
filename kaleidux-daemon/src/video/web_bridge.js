(() => {
    "use strict";
    const audioListeners = new Set();
    const spectrum = new Array(128).fill(0);
    let primaryTarget = null;
    let previousPointer = [0, 0, 0, 0];

    window.wallpaperRegisterAudioListener = (listener) => {
        if (typeof listener === "function") {
            audioListeners.add(listener);
        }
    };

    function updateAudio(bands) {
        for (let i = 0; i < 64; ++i) {
            spectrum[i] = spectrum[64 + i] = bands[i] ?? 0;
        }
        for (const listener of audioListeners) {
            try {
                listener(spectrum.slice());
            } catch (error) {
                console.error(error);
            }
        }
    }

    function updatePointer(pointer) {
        const [x, y, left, right] = pointer;
        const clientX = Math.min(innerWidth - 1, x * innerWidth);
        const clientY = Math.min(innerHeight - 1, y * innerHeight);
        const target = document.elementFromPoint(clientX, clientY);
        const options = {
            clientX, clientY, bubbles: true, cancelable: true,
            buttons: (left > 0.5 ? 1 : 0) | (right > 0.5 ? 2 : 0),
            pointerId: 1, pointerType: "mouse", isPrimary: true,
        };
        if (target && (x !== previousPointer[0] || y !== previousPointer[1])) {
            target.dispatchEvent(new PointerEvent("pointermove", options));
            target.dispatchEvent(new MouseEvent("mousemove", options));
        }
        for (const [index, button] of [[2, 0], [3, 2]]) {
            const down = pointer[index] > 0.5;
            if (target && down !== (previousPointer[index] > 0.5)) {
                const eventOptions = {...options, button};
                target.dispatchEvent(new PointerEvent(down ? "pointerdown" : "pointerup", eventOptions));
                target.dispatchEvent(new MouseEvent(down ? "mousedown" : "mouseup", eventOptions));
            }
            if (button === 0 && down !== (previousPointer[index] > 0.5)) {
                if (!down && target && target === primaryTarget) {
                    target.dispatchEvent(new MouseEvent("click", {...options, button, detail: 1}));
                }
                primaryTarget = down ? target : null;
            }
        }
        previousPointer = pointer;
    }

    window.__kaleiduxWallpaper = {
        start({fps, properties}) {
            window.wallpaperPropertyListener?.applyGeneralProperties?.({fps: {value: fps}});
            window.wallpaperPropertyListener?.applyUserProperties?.(properties);
        },
        pause(paused) {
            window.wallpaperPropertyListener?.setPaused?.(paused);
        },
        input({audio, pointer, pointer_edges = []}) {
            if (audio) updateAudio(audio);
            for (const edge of pointer_edges) updatePointer(edge);
            if (pointer) updatePointer(pointer);
        },
    };
})();
