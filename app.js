const catButton = document.getElementById("catButton");
const meowValue = document.getElementById("meowValue");
const meowPulse = document.getElementById("meowPulse");
const meowCounter = document.querySelector(".meow-counter");
const meowStreak = document.getElementById("meowStreak");
const meowAudio = document.getElementById("meowAudio");

let count = 0;
let audioContext;
let lastClickTime = 0;
let streak = 0;
let streakTimeout;

const createAudioContext = () => {
  if (!audioContext) {
    audioContext = new (window.AudioContext || window.webkitAudioContext)();
  }
  if (audioContext.state === "suspended") {
    audioContext.resume();
  }
};

const playSynthMeow = () => {
  createAudioContext();

  const now = audioContext.currentTime;
  const osc = audioContext.createOscillator();
  const gain = audioContext.createGain();
  const filter = audioContext.createBiquadFilter();

  osc.type = "sawtooth";
  filter.type = "lowpass";
  filter.frequency.setValueAtTime(1200, now);
  filter.Q.value = 0.8;

  osc.frequency.setValueAtTime(820, now);
  osc.frequency.exponentialRampToValueAtTime(480, now + 0.18);
  osc.frequency.exponentialRampToValueAtTime(620, now + 0.32);

  gain.gain.setValueAtTime(0.0001, now);
  gain.gain.exponentialRampToValueAtTime(0.12, now + 0.03);
  gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.38);

  osc.connect(filter);
  filter.connect(gain);
  gain.connect(audioContext.destination);

  osc.start(now);
  osc.stop(now + 0.4);
};

const playMeow = () => {
  if (meowAudio?.src || meowAudio?.querySelector("source")?.src) {
    meowAudio.currentTime = 0;
    meowAudio.play().catch(() => playSynthMeow());
    return;
  }

  playSynthMeow();
};

const bumpCounter = () => {
  meowCounter.classList.remove("bump");
  requestAnimationFrame(() => {
    meowCounter.classList.add("bump");
  });
};

const showPulse = () => {
  meowPulse.classList.remove("show");
  requestAnimationFrame(() => {
    meowPulse.classList.add("show");
  });
};

const updateStreak = () => {
  const now = Date.now();
  const gap = now - lastClickTime;
  lastClickTime = now;

  if (gap < 900) {
    streak += 1;
  } else {
    streak = 1;
  }

  const speedBoost = Math.min(1 + streak * 0.04, 1.4);
  catButton.style.transform = `scale(${speedBoost})`;

  clearTimeout(streakTimeout);
  meowStreak.textContent = `x${streak}`;
  meowStreak.classList.add("show");

  streakTimeout = setTimeout(() => {
    streak = 0;
    meowStreak.classList.remove("show");
    catButton.style.transform = "";
  }, 1200);
};

catButton.addEventListener("click", () => {
  count += 1;
  meowValue.textContent = String(count);

  playMeow();

  catButton.classList.remove("meow");
  requestAnimationFrame(() => {
    catButton.classList.add("meow");
  });

  showPulse();
  bumpCounter();
  updateStreak();
});

meowPulse.addEventListener("animationend", () => {
  meowPulse.classList.remove("show");
});

catButton.addEventListener("animationend", () => {
  catButton.classList.remove("meow");
});
