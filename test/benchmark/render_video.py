#!/usr/bin/env python3
"""
Renders a high-production 60 FPS MP4 video comparing all 5 streaming engines
with bouncing balls, glowing trails, real-time counters, and victory badges.
Outputs: test/benchmark/rekuiper_benchmark_race.mp4
"""
import subprocess
import math
import os
import sys
from PIL import Image, ImageDraw, ImageFont

WIDTH = 1920
HEIGHT = 1080
FPS = 60
TOTAL_RECORDS = 500_000
DURATION_SEC = 13.0  # Duration of video
TOTAL_FRAMES = int(FPS * DURATION_SEC)

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rekuiper_benchmark_race.mp4")

# Color palette (RGB)
BG_COLOR = (11, 16, 28)
TRACK_BG = (17, 24, 39)
TRACK_LINE = (38, 50, 72)
TEXT_WHITE = (248, 250, 252)
TEXT_MUTED = (148, 163, 184)

# Lanes config
LANES = [
    {
        "name": "rekuiper",
        "title": "rekuiper (Pure Rust)",
        "sub": "Single Core | RAM: 8.2 MB | 0 Drops",
        "color": (255, 87, 34),       # Vibrant Orange / Rust
        "light_color": (255, 171, 145),
        "elapsed": 1.1756,
        "eps": 425308,
        "drops": 0,
        "bounce_freq": 3.8,           # Bounces per second
    },
    {
        "name": "flink",
        "title": "Apache Flink (Java / JVM)",
        "sub": "Single Core | RAM: 1,022 MB (1.02 GB)",
        "color": (0, 229, 255),        # Cyan
        "light_color": (178, 255, 255),
        "elapsed": 2.1440,
        "eps": 233209,
        "drops": 0,
        "bounce_freq": 2.2,
    },
    {
        "name": "telegraf",
        "title": "Telegraf (Go)",
        "sub": "Single Core | RAM: ~50 MB",
        "color": (0, 230, 118),        # Emerald Green
        "light_color": (185, 246, 202),
        "elapsed": 8.1941,
        "eps": 61019,
        "drops": 0,
        "bounce_freq": 0.9,
    },
    {
        "name": "ekuiper",
        "title": "Upstream Go eKuiper",
        "sub": "Single Core | RAM: ~45 MB | 72,921 DROPS",
        "color": (255, 179, 0),        # Amber
        "light_color": (255, 224, 130),
        "elapsed": 11.2904,
        "eps": 44287,
        "drops": 72921,
        "bounce_freq": 0.65,
    },
    {
        "name": "benthos",
        "title": "Redpanda Connect (Benthos)",
        "sub": "Single Core | RAM: ~38 MB",
        "color": (224, 64, 251),       # Magenta
        "light_color": (244, 143, 177),
        "elapsed": 19.2360,
        "eps": 25993,
        "drops": 0,
        "bounce_freq": 0.38,
    }
]

# Track Geometry
PADDING_LEFT = 480
PADDING_RIGHT = 340
TRACK_WIDTH = WIDTH - PADDING_LEFT - PADDING_RIGHT
LANE_HEIGHT = 160
TOP_OFFSET = 180
BALL_RADIUS = 24

def get_font(size, bold=False):
    # Try system fonts
    font_names = [
        "arialbd.ttf" if bold else "arial.ttf",
        "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf",
        "SegoeUI-Bold.ttf" if bold else "SegoeUI.ttf"
    ]
    for name in font_names:
        try:
            return ImageFont.truetype(name, size)
        except Exception:
            pass
    return ImageFont.load_default()

def render_video():
    print("=" * 70)
    print(f"Rendering 60 FPS Benchmark Video: {OUTPUT_FILE}")
    print(f"Resolution: {WIDTH}x{HEIGHT} | Frames: {TOTAL_FRAMES} | Duration: {DURATION_SEC}s")
    print("=" * 70)

    font_title = get_font(44, bold=True)
    font_sub = get_font(22, bold=False)
    font_timer = get_font(38, bold=True)
    font_lane_title = get_font(28, bold=True)
    font_lane_sub = get_font(18, bold=False)
    font_metrics = get_font(26, bold=True)
    font_badge = get_font(22, bold=True)

    # Launch FFmpeg process piping raw RGB frames
    ffmpeg_cmd = [
        "ffmpeg", "-y",
        "-f", "rawvideo",
        "-vcodec", "rawvideo",
        "-s", f"{WIDTH}x{HEIGHT}",
        "-pix_fmt", "rgb24",
        "-r", str(FPS),
        "-i", "-",
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "17",
        "-pix_fmt", "yuv420p",
        OUTPUT_FILE
    ]

    proc = subprocess.Popen(ffmpeg_cmd, stdin=subprocess.PIPE, stderr=subprocess.DEVNULL)

    # Particle system for impacts
    sparks = []

    for f_idx in range(TOTAL_FRAMES):
        sim_time = f_idx / FPS
        img = Image.new("RGB", (WIDTH, HEIGHT), BG_COLOR)
        draw = ImageDraw.Draw(img)

        # 1. Header & Title Banner
        draw.text((60, 45), "STREAMING ENGINE BENCHMARK RACE", font=font_title, fill=TEXT_WHITE)
        draw.text((60, 105), "500,000 Wide-Schema Telemetry Events on Single CPU Core (Linux x86_64)", font=font_sub, fill=TEXT_MUTED)

        # Header Timer
        mins = int(sim_time // 60)
        secs = int(sim_time % 60)
        ms = int((sim_time % 1) * 1000)
        time_str = f"ELAPSED: {mins:02d}:{secs:02d}.{ms:03d}"
        draw.rounded_rectangle([WIDTH - 440, 50, WIDTH - 60, 120], radius=14, fill=(15, 23, 42), outline=(56, 189, 248), width=2)
        draw.text((WIDTH - 415, 65), time_str, font=font_timer, fill=(56, 189, 248))

        # 2. Render each track lane
        for idx, lane in enumerate(LANES):
            y_top = TOP_OFFSET + idx * LANE_HEIGHT
            y_center = y_top + LANE_HEIGHT // 2
            y_bottom = y_top + LANE_HEIGHT

            # Lane background & separator
            bg = TRACK_BG if idx % 2 == 0 else (13, 19, 33)
            draw.rectangle([0, y_top, WIDTH, y_bottom], fill=bg)
            draw.line([(0, y_bottom), (WIDTH, y_bottom)], fill=(255, 255, 255, 15), width=1)

            # Left side: Engine Information
            draw.text((50, y_center - 28), lane["title"], font=font_lane_title, fill=lane["color"])
            draw.text((50, y_center + 10), lane["sub"], font=font_lane_sub, fill=TEXT_MUTED)

            # Track rails
            rail_x1 = PADDING_LEFT
            rail_x2 = PADDING_LEFT + TRACK_WIDTH
            draw.line([(rail_x1, y_center), (rail_x2, y_center)], fill=TRACK_LINE, width=6)

            # Left/Right impact bumpers
            draw.rectangle([rail_x1 - 8, y_center - 24, rail_x1, y_center + 24], fill=(100, 116, 139))
            draw.rectangle([rail_x2, y_center - 24, rail_x2 + 8, y_center + 24], fill=(100, 116, 139))

            # Progress calculation
            progress = min(1.0, sim_time / lane["elapsed"])
            processed = int(progress * TOTAL_RECORDS)
            finished = progress >= 1.0

            # Ball position based on bounce frequency
            # Triangle wave oscillation between 0 and TRACK_WIDTH
            if not finished:
                period = 1.0 / lane["bounce_freq"]
                cycle = (sim_time % period) / period
                if cycle < 0.5:
                    bx = rail_x1 + (cycle * 2) * TRACK_WIDTH
                    dx = 1
                else:
                    bx = rail_x2 - ((cycle - 0.5) * 2) * TRACK_WIDTH
                    dx = -1

                # Detect bumper hit to spawn sparks
                if cycle < 0.05 or (0.5 <= cycle < 0.55):
                    if len(sparks) < 200:
                        sparks.append({
                            "x": bx,
                            "y": y_center,
                            "color": (239, 68, 68) if lane["drops"] > 0 and math.sin(sim_time * 10) > 0 else lane["color"],
                            "life": 12
                        })
            else:
                # Stop at the right finish line
                bx = rail_x2
                dx = 0

            # Draw Motion Trail
            if not finished:
                for t_step in range(1, 6):
                    trail_x = bx - dx * (t_step * 14)
                    alpha_r = int(BALL_RADIUS * (1.0 - t_step * 0.15))
                    t_color = (
                        int(lane["color"][0] * (1.0 - t_step * 0.18)),
                        int(lane["color"][1] * (1.0 - t_step * 0.18)),
                        int(lane["color"][2] * (1.0 - t_step * 0.18)),
                    )
                    draw.ellipse([trail_x - alpha_r, y_center - alpha_r, trail_x + alpha_r, y_center + alpha_r], fill=t_color)

            # Draw Outer Glowing Ball
            glow_r = BALL_RADIUS + 8
            draw.ellipse([bx - glow_r, y_center - glow_r, bx + glow_r, y_center + glow_r], fill=lane["color"])

            # Inner Core Ball
            draw.ellipse([bx - BALL_RADIUS, y_center - BALL_RADIUS, bx + BALL_RADIUS, y_center + BALL_RADIUS], fill=lane["light_color"])
            draw.ellipse([bx - BALL_RADIUS // 2, y_center - BALL_RADIUS // 2, bx + BALL_RADIUS // 2, y_center + BALL_RADIUS // 2], fill=(255, 255, 255))

            # Right side: Real-time Stats & Badges
            stats_x = rail_x2 + 36
            draw.text((stats_x, y_center - 24), f"{processed:,} events", font=font_metrics, fill=TEXT_WHITE)

            if finished:
                if idx == 0:
                    badge_text = "🏆 1st PLACE (1.176s - 425k eps)"
                    badge_color = (34, 197, 94)
                elif idx == 1:
                    badge_text = "🥈 2nd PLACE (2.144s - 233k eps)"
                    badge_color = (56, 189, 248)
                elif idx == 2:
                    badge_text = "🥉 3rd PLACE (8.194s - 61k eps)"
                    badge_color = (250, 204, 21)
                else:
                    badge_text = f"FINISHED in {lane['elapsed']:.2f}s"
                    badge_color = (148, 163, 184)
                draw.text((stats_x, y_center + 10), badge_text, font=font_badge, fill=badge_color)
            elif lane["drops"] > 0 and sim_time > 2.0:
                draw.text((stats_x, y_center + 10), f"⚠️ {lane['drops']:,} DROPS (14.6%)", font=font_badge, fill=(239, 68, 68))
            else:
                pct = int(progress * 100)
                draw.text((stats_x, y_center + 10), f"Processing... {pct}%", font=font_lane_sub, fill=TEXT_MUTED)

        # 3. Draw and update sparks
        new_sparks = []
        for sp in sparks:
            draw.rectangle([sp["x"] - 4, sp["y"] - 4, sp["x"] + 4, sp["y"] + 4], fill=sp["color"])
            sp["life"] -= 1
            if sp["life"] > 0:
                new_sparks.append(sp)
        sparks = new_sparks

        # Send raw frame to FFmpeg stdin
        proc.stdin.write(img.tobytes())

        if (f_idx + 1) % 120 == 0:
            pct_done = int((f_idx + 1) / TOTAL_FRAMES * 100)
            print(f"Rendered {f_idx + 1}/{TOTAL_FRAMES} frames ({pct_done}%)...")

    proc.stdin.close()
    proc.wait()
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\nSUCCESS! 60 FPS Video created: {OUTPUT_FILE} ({file_size_mb:.2f} MB)")

if __name__ == "__main__":
    render_video()
