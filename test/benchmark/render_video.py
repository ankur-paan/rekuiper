#!/usr/bin/env python3
"""
Renders a high-production 60 FPS MP4 video comparing all 5 streaming engines
in Material Design 3 (M3) Light Theme with bouncing balls, realistic ballistic sparks,
3D glossy spheres, and empirical benchmark data extrapolated to 1,000,000 events.
Outputs: test/benchmark/rekuiper_benchmark_race_1m.mp4
"""
import subprocess
import math
import os
import sys
import random
from PIL import Image, ImageDraw, ImageFont

WIDTH = 1920
HEIGHT = 1080
FPS = 60
TOTAL_RECORDS = 1_000_000
DURATION_SEC = 25.5  # 25.5 second video for 1M events
TOTAL_FRAMES = int(FPS * DURATION_SEC)

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rekuiper_benchmark_race_1m.mp4")

# Material Design 3 Light Theme Color Palette (RGB)
BG_COLOR = (248, 249, 252)          # M3 Surface Container Low
TRACK_BG_EVEN = (255, 255, 255)      # Pure White lane
TRACK_BG_ODD = (241, 244, 249)       # M3 Surface Container
TRACK_LINE = (203, 213, 225)         # M3 Subtle Rail
TEXT_PRIMARY = (15, 23, 42)          # Dark Slate (High contrast)
TEXT_MUTED = (100, 116, 139)         # Muted Slate
BORDER_COLOR = (226, 232, 240)

# Lanes config extrapolated to 1,000,000 records
LANES = [
    {
        "name": "rekuiper",
        "title": "rekuiper (Pure Rust)",
        "sub": "Single Core | RAM: 8.2 MB | 0 Drops",
        "color": (194, 65, 12),       # Vibrant Terracotta Rust
        "light_color": (255, 112, 67),
        "elapsed": 2.3512,            # 1,000,000 / 425,308 eps
        "eps": 425308,
        "drops": 0,
        "bounce_freq": 2.8,           # Bounces per second
    },
    {
        "name": "flink",
        "title": "Apache Flink (Java / JVM)",
        "sub": "Single Core | RAM: 1,022 MB (1.02 GB)",
        "color": (2, 132, 199),        # Cyan Blue
        "light_color": (56, 189, 248),
        "elapsed": 4.2880,            # 1,000,000 / 233,209 eps
        "eps": 233209,
        "drops": 0,
        "bounce_freq": 1.6,
    },
    {
        "name": "telegraf",
        "title": "Telegraf (Go)",
        "sub": "Single Core | RAM: ~50 MB",
        "color": (5, 150, 105),        # Emerald Green
        "light_color": (52, 211, 153),
        "elapsed": 16.3882,           # 1,000,000 / 61,019 eps
        "eps": 61019,
        "drops": 0,
        "bounce_freq": 0.65,
    },
    {
        "name": "ekuiper",
        "title": "eKuiper",
        "sub": "Single Core | RAM: ~45 MB | 145k DROPS",
        "color": (217, 119, 6),        # Warm Amber
        "light_color": (251, 191, 36),
        "elapsed": 22.5809,           # 1,000,000 / 44,287 eps
        "eps": 44287,
        "drops": 145842,
        "bounce_freq": 0.48,
    },
    {
        "name": "benthos",
        "title": "Redpanda Connect (Benthos)",
        "sub": "Single Core | RAM: ~38 MB",
        "color": (124, 58, 237),       # Royal Violet
        "light_color": (167, 139, 250),
        "elapsed": 38.4720,           # 1,000,000 / 25,993 eps
        "eps": 25993,
        "drops": 0,
        "bounce_freq": 0.28,
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

def spawn_sparks(x, y, color, count=16, is_drop=False, direction=0):
    new_sparks = []
    for _ in range(count):
        speed = random.uniform(3.0, 9.0)
        if direction == -1:
            # Bounced off right bumper -> fly leftward with angular spread
            angle = math.pi + random.uniform(-0.8, 0.8)
        elif direction == 1:
            # Bounced off left bumper -> fly rightward with angular spread
            angle = random.uniform(-0.8, 0.8)
        else:
            angle = random.uniform(0, math.pi * 2)

        spark_color = (220, 38, 38) if is_drop else (color if random.random() > 0.4 else (245, 158, 11))
        new_sparks.append({
            "x": float(x),
            "y": float(y),
            "vx": math.cos(angle) * speed,
            "vy": math.sin(angle) * speed,
            "color": spark_color,
            "life": 1.0,
            "decay": random.uniform(2.0, 3.2),  # lives ~0.35s - 0.5s
            "size": random.uniform(3.0, 5.5)
        })
    return new_sparks

def render_video():
    print("=" * 70)
    print(f"Rendering 60 FPS M3 Light Benchmark Video: {OUTPUT_FILE}")
    print(f"Resolution: {WIDTH}x{HEIGHT} | Frames: {TOTAL_FRAMES} | Duration: {DURATION_SEC}s")
    print("=" * 70)

    font_title = get_font(42, bold=True)
    font_sub = get_font(21, bold=False)
    font_timer = get_font(36, bold=True)
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

    # Global particle system for impact sparks
    sparks = []
    # Track previous bumper state to trigger sparks on impact transition
    prev_bumper_states = [None] * len(LANES)

    for f_idx in range(TOTAL_FRAMES):
        sim_time = f_idx / FPS
        dt = 1.0 / FPS

        img = Image.new("RGB", (WIDTH, HEIGHT), BG_COLOR)
        draw = ImageDraw.Draw(img)

        # 1. Header & Title Banner (M3 Light)
        draw.text((60, 48), "STREAMING ENGINE BENCHMARK RACE - 1,000,000 EVENTS", font=font_title, fill=TEXT_PRIMARY)
        draw.text((60, 108), "1,000,000 Wide-Schema Telemetry Events on Single CPU Core (Empirical Extrapolation)", font=font_sub, fill=TEXT_MUTED)

        # Header Timer Pill (M3 Tonal Container)
        mins = int(sim_time // 60)
        secs = int(sim_time % 60)
        ms = int((sim_time % 1) * 1000)
        time_str = f"ELAPSED: {mins:02d}:{secs:02d}.{ms:03d}"
        draw.rounded_rectangle([WIDTH - 460, 50, WIDTH - 60, 122], radius=18, fill=(237, 242, 252), outline=(211, 227, 253), width=2)
        draw.text((WIDTH - 430, 66), time_str, font=font_timer, fill=(11, 87, 208))

        # 2. Render each track lane
        for idx, lane in enumerate(LANES):
            y_top = TOP_OFFSET + idx * LANE_HEIGHT
            y_center = y_top + LANE_HEIGHT // 2
            y_bottom = y_top + LANE_HEIGHT

            # Lane background & separator (M3 alternating bands)
            bg = TRACK_BG_EVEN if idx % 2 == 0 else TRACK_BG_ODD
            draw.rectangle([0, y_top, WIDTH, y_bottom], fill=bg)
            draw.line([(0, y_bottom), (WIDTH, y_bottom)], fill=BORDER_COLOR, width=1)

            # Left side: Engine Information
            draw.text((50, y_center - 28), lane["title"], font=font_lane_title, fill=lane["color"])
            draw.text((50, y_center + 10), lane["sub"], font=font_lane_sub, fill=TEXT_MUTED)

            # Track rails (Clean M3 rail)
            rail_x1 = PADDING_LEFT
            rail_x2 = PADDING_LEFT + TRACK_WIDTH
            draw.line([(rail_x1, y_center), (rail_x2, y_center)], fill=TRACK_LINE, width=6)

            # Left/Right impact bumpers (Slate rounded pill stops)
            draw.rounded_rectangle([rail_x1 - 10, y_center - 24, rail_x1, y_center + 24], radius=4, fill=(148, 163, 184))
            draw.rounded_rectangle([rail_x2, y_center - 24, rail_x2 + 10, y_center + 24], radius=4, fill=(148, 163, 184))

            # Progress calculation
            progress = min(1.0, sim_time / lane["elapsed"])
            processed = int(progress * TOTAL_RECORDS)
            finished = progress >= 1.0

            # Ball position based on bounce frequency
            if not finished:
                period = 1.0 / lane["bounce_freq"]
                cycle = (sim_time % period) / period
                if cycle < 0.5:
                    bx = rail_x1 + (cycle * 2) * TRACK_WIDTH
                    dx = 1
                else:
                    bx = rail_x2 - ((cycle - 0.5) * 2) * TRACK_WIDTH
                    dx = -1

                # Detect bumper hit transitions to spawn directional sparks
                cur_bumper = "right" if cycle >= 0.48 and cycle < 0.52 else ("left" if cycle >= 0.98 or cycle < 0.02 else None)
                if cur_bumper and cur_bumper != prev_bumper_states[idx]:
                    direction = -1 if cur_bumper == "right" else 1
                    contact_x = rail_x2 if cur_bumper == "right" else rail_x1
                    sparks.extend(spawn_sparks(contact_x, y_center, lane["color"], count=14, is_drop=(lane["drops"] > 0), direction=direction))
                prev_bumper_states[idx] = cur_bumper
            else:
                # Dock cleanly at the finish line bumper
                bx = rail_x2
                dx = 0
                if prev_bumper_states[idx] != "finished":
                    # Spawn victory burst upon crossing finish line
                    sparks.extend(spawn_sparks(rail_x2, y_center, lane["color"], count=24, is_drop=False, direction=0))
                    prev_bumper_states[idx] = "finished"

            # Draw Motion Trail (Smooth alpha fade behind the ball)
            if not finished:
                for t_step in range(1, 6):
                    trail_x = bx - dx * (t_step * 14)
                    alpha_factor = 1.0 - (t_step * 0.16)
                    t_radius = int(BALL_RADIUS * alpha_factor)
                    # Blend lane color towards track background for smooth anti-aliased trail
                    bg_color = bg
                    t_color = (
                        int(lane["color"][0] * alpha_factor * 0.4 + bg_color[0] * (1.0 - alpha_factor * 0.4)),
                        int(lane["color"][1] * alpha_factor * 0.4 + bg_color[1] * (1.0 - alpha_factor * 0.4)),
                        int(lane["color"][2] * alpha_factor * 0.4 + bg_color[2] * (1.0 - alpha_factor * 0.4)),
                    )
                    draw.ellipse([trail_x - t_radius, y_center - t_radius, trail_x + t_radius, y_center + t_radius], fill=t_color)

            # Draw Soft Elevation Shadow (M3 light theme)
            shadow_offset = 4
            shadow_r = BALL_RADIUS + 2
            shadow_color = (
                int(bg[0] * 0.85),
                int(bg[1] * 0.85),
                int(bg[2] * 0.88),
            )
            draw.ellipse([bx - shadow_r, y_center + shadow_offset - shadow_r, bx + shadow_r, y_center + shadow_offset + shadow_r], fill=shadow_color)

            # Draw Main Ball Sphere (Vibrant M3 color)
            draw.ellipse([bx - BALL_RADIUS, y_center - BALL_RADIUS, bx + BALL_RADIUS, y_center + BALL_RADIUS], fill=lane["color"])

            # Draw 3D Specular Highlight (Inner glossy depth)
            high_offset_x = 0 if finished else int(dx * 4)
            draw.ellipse([bx - high_offset_x - 10, y_center - 10, bx - high_offset_x + 6, y_center + 6], fill=lane["light_color"])
            draw.ellipse([bx - high_offset_x - 7, y_center - 7, bx - high_offset_x + 1, y_center + 1], fill=(255, 255, 255))

            # Right side: Real-time Stats & Badges
            stats_x = rail_x2 + 36
            metric_color = lane["color"] if finished else TEXT_PRIMARY
            draw.text((stats_x, y_center - 24), f"{processed:,} events", font=font_metrics, fill=metric_color)

            if finished:
                if idx == 0:
                    badge_text = "🏆 1st PLACE (2.35s · 425k eps)"
                    badge_color = (22, 163, 74)
                elif idx == 1:
                    badge_text = "🥈 2nd PLACE (4.29s · 233k eps)"
                    badge_color = (2, 132, 199)
                elif idx == 2:
                    badge_text = "🥉 3rd PLACE (16.39s · 61k eps)"
                    badge_color = (202, 138, 4)
                else:
                    badge_text = f"FINISHED in {lane['elapsed']:.2f}s"
                    badge_color = (100, 116, 139)
                draw.text((stats_x, y_center + 10), badge_text, font=font_badge, fill=badge_color)
            elif lane["drops"] > 0 and sim_time > 3.0:
                draw.text((stats_x, y_center + 10), f"⚠️ 145k DROPS (14.6%)", font=font_badge, fill=(220, 38, 38))
            else:
                pct = int(progress * 100)
                draw.text((stats_x, y_center + 10), f"Processing... {pct}%", font=font_lane_sub, fill=TEXT_MUTED)

        # 3. Always update & draw sparks across all lanes
        # Even when balls finish or stop, sparks NEVER freeze; they continue flying,
        # decelerating, and fading out smoothly!
        new_sparks = []
        for sp in sparks:
            # Physical motion with gentle air drag
            sp["x"] += sp["vx"]
            sp["y"] += sp["vy"]
            sp["vx"] *= 0.95
            sp["vy"] *= 0.95
            sp["life"] -= dt * sp["decay"]

            if sp["life"] > 0:
                r = sp["size"] * sp["life"]
                if r > 0.5:
                    draw.ellipse([sp["x"] - r, sp["y"] - r, sp["x"] + r, sp["y"] + r], fill=sp["color"])
                new_sparks.append(sp)
        sparks = new_sparks

        # Send raw frame to FFmpeg stdin
        proc.stdin.write(img.tobytes())

        if (f_idx + 1) % 150 == 0:
            pct_done = int((f_idx + 1) / TOTAL_FRAMES * 100)
            print(f"Rendered {f_idx + 1}/{TOTAL_FRAMES} frames ({pct_done}%)...")

    proc.stdin.close()
    proc.wait()
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\nSUCCESS! 60 FPS M3 Light Video created: {OUTPUT_FILE} ({file_size_mb:.2f} MB)")

if __name__ == "__main__":
    render_video()
