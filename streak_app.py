import streamlit as st
import sqlite3
import pandas as pd
import altair as alt
from datetime import date, datetime
from pybaseball import playerid_lookup
import requests
from concurrent.futures import ThreadPoolExecutor

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(layout="wide")
st.title("🔥 MLB Streak + Regression Analyzer")

ODDS_API_KEY = "46860b5ed2ba75acaa783a1bb8253723"
TODAY = date.today().strftime("%Y-%m-%d")

BOOKMAKER_MAP = {
    "PrizePicks": "prizepicks",
    "Underdog": "underdog",
}

SUPPORTED_PROP_MARKETS = {
    "Hits": "batter_hits",
    "Singles": "batter_singles",
    "Home Runs": "batter_home_runs",
    "Rbi": "batter_rbis",
    "Runs": "batter_runs_scored",
    "Walks": "batter_walks",
    "Strikeouts": "batter_strikeouts",
    "Hits + Runs + Rbi": "batter_hits_runs_rbis",
    "Fantasy Points": "batter_fantasy_score",
}

ALL_STATS_SCAN = [
    "Hits",
    "Singles",
    "Home Runs",
    "Rbi",
    "Runs",
    "Walks",
    "Strikeouts",
    "Hits + Runs + Rbi"
]

HITS_GROUP = [
    "Hits",
    "Singles",
    "Home Runs",
    "Rbi",
    "Runs",
    "Hits + Runs + Rbi"
]

FANTASY_SCORING = {
    "PrizePicks": {
        "Hitter": {
            "single": 3.0,
            "double": 5.0,
            "triple": 8.0,
            "home_run": 10.0,
            "run": 2.0,
            "rbi": 2.0,
            "walk": 2.0,
            "hbp": 2.0,
            "stolen_base": 5.0,
        },
        "Pitcher": {
            "win": 6.0,
            "quality_start": 4.0,
            "earned_run": -3.0,
            "strikeout": 3.0,
            "out": 1.0,
        },
    },
    "Underdog": {
        "Hitter": {
            "single": 3.0,
            "double": 6.0,
            "triple": 8.0,
            "home_run": 10.0,
            "walk": 3.0,
            "hbp": 3.0,
            "rbi": 2.0,
            "run": 2.0,
            "stolen_base": 4.0,
        },
        "Pitcher": {
            "win": 5.0,
            "quality_start": 5.0,
            "strikeout": 3.0,
            "inning_pitched": 3.0,
            "earned_run": -3.0,
        },
    },
}

SPORTSBOOK_ERROR_TAG = "Sorry bro, look on the app"
MLB_LOOKUP_ERROR_TAG = "Player not found in MLB lookup"

# =========================================================
# DATABASE
# =========================================================
conn = sqlite3.connect("mlb_streaks.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS games (
    player_id INTEGER,
    player_name TEXT,
    game_date TEXT,
    events TEXT,
    PRIMARY KEY (player_id, game_date, events)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS props_cache (
    scan_date TEXT,
    bookmaker_key TEXT,
    event_id TEXT,
    market_key TEXT,
    player_name TEXT,
    line REAL,
    over_price REAL,
    under_price REAL,
    last_update TEXT,
    PRIMARY KEY (scan_date, bookmaker_key, event_id, market_key, player_name, line)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS props_cache_meta (
    scan_date TEXT,
    bookmaker_key TEXT,
    refreshed_at TEXT,
    PRIMARY KEY (scan_date, bookmaker_key)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS scan_results_players (
    scan_date TEXT,
    bookmaker_key TEXT,
    normalized_name TEXT,
    player_name TEXT,
    team TEXT,
    matchup TEXT,
    book TEXT,
    player_id INTEGER,
    error TEXT,
    PRIMARY KEY (scan_date, bookmaker_key, normalized_name)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS scan_results_stats (
    scan_date TEXT,
    bookmaker_key TEXT,
    normalized_name TEXT,
    stat_index INTEGER,
    stat TEXT,
    line REAL,
    direction TEXT,
    current INTEGER,
    mode TEXT,
    fantasy_book TEXT,
    PRIMARY KEY (scan_date, bookmaker_key, normalized_name, stat_index)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS scan_results_meta (
    scan_date TEXT,
    bookmaker_key TEXT,
    refreshed_at TEXT,
    result_count INTEGER,
    PRIMARY KEY (scan_date, bookmaker_key)
)
""")

conn.commit()

# =========================================================
# HELPERS
# =========================================================
def normalize_name(name):
    if not name:
        return ""
    x = str(name).strip().lower()
    x = x.replace(".", "").replace("’", "'").replace("`", "'")
    suffixes = [" jr", " sr", " ii", " iii", " iv", " v"]
    for s in suffixes:
        if x.endswith(s):
            x = x[:-len(s)]
    x = " ".join(x.split())
    return x

def get_player_id(name):
    try:
        parts = name.split(" ")
        if len(parts) < 2:
            return None
        first = parts[0]
        last = parts[-1]
        df = playerid_lookup(last, first)
        if df.empty:
            return None
        return int(df.iloc[0]["key_mlbam"])
    except:
        return None

def get_headshot(player_id):
    if not player_id:
        return "https://img.mlbstatic.com/mlb-photos/image/upload/v1/people/missing/headshot/67/current.png"
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/v1/people/{int(player_id)}/headshot/67/current.png"

def innings_string_to_float(ip_value):
    try:
        s = str(ip_value)
        if "." not in s:
            return float(s)
        whole, frac = s.split(".")
        whole = int(whole)
        frac = int(frac)
        if frac == 0:
            return float(whole)
        if frac == 1:
            return whole + (1 / 3)
        if frac == 2:
            return whole + (2 / 3)
        return float(whole)
    except:
        return 0.0

def streak_rating(current, past):
    if not past:
        return "⚪ Coin Flip ⚖️", 0.5

    mx = max(past)
    prob = 0.70 - (current / (mx + 1)) * 0.4
    prob = max(0.05, min(0.95, prob))

    if prob < 0.40:
        return "🔴 Weak ⚠️", prob
    elif prob < 0.65:
        return "⚪ Coin Flip ⚖️", prob
    return "🟢 Strong ✅", prob

def build_result_streaks(game_stats):
    streak = 0
    streak_list = []
    for v in game_stats["cleared"][::-1]:
        streak = streak + 1 if v else 0
        streak_list.append(streak)
    game_stats["streak"] = list(reversed(streak_list))

    current = 0
    for v in game_stats["cleared"]:
        if v:
            current += 1
        else:
            break

    past = []
    temp = 0
    for v in game_stats["cleared"][::-1]:
        if v:
            temp += 1
        else:
            if temp > 0:
                past.append(temp)
            temp = 0
    if temp > 0:
        past.append(temp)

    return game_stats, current, past

# =========================================================
# DATA LOADERS
# =========================================================
@st.cache_data(show_spinner=False, ttl=60 * 60 * 6)
def load_hitter_data(player_id):
    seasons = ["2026", "2025", "2024", "2023", "2022"]

    def fetch(season):
        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
        params = {"stats": "gameLog", "group": "hitting", "season": season}
        try:
            r = requests.get(url, params=params, timeout=12).json()
            splits = r.get("stats", [{}])[0].get("splits", [])
            rows = []

            for g in splits:
                stat = g.get("stat", {})
                hits = int(stat.get("hits", 0))
                doubles = int(stat.get("doubles", 0))
                triples = int(stat.get("triples", 0))
                home_runs = int(stat.get("homeRuns", 0))
                runs = int(stat.get("runs", 0))
                rbi = int(stat.get("rbi", 0))
                walks = int(stat.get("baseOnBalls", 0))
                strikeouts = int(stat.get("strikeOuts", 0))
                hbp = int(stat.get("hitByPitch", 0))
                sb = int(stat.get("stolenBases", 0))

                rows.append({
                    "game_date": g.get("date"),
                    "hits": hits,
                    "doubles": doubles,
                    "triples": triples,
                    "home_runs": home_runs,
                    "rbi": rbi,
                    "walks": walks,
                    "strikeouts": strikeouts,
                    "runs": runs,
                    "hbp": hbp,
                    "stolen_bases": sb,
                    "singles": max(0, hits - doubles - triples - home_runs),
                    "hits_runs_rbi": hits + runs + rbi,
                    "ab": int(stat.get("atBats", 0))
                })
            return rows
        except:
            return []

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(fetch, seasons)

    all_rows = []
    for r in results:
        all_rows.extend(r)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df = df.dropna(subset=["game_date"])
    return df.sort_values("game_date", ascending=False)

@st.cache_data(show_spinner=False, ttl=60 * 60 * 6)
def load_pitcher_data(player_id):
    seasons = ["2026", "2025", "2024", "2023", "2022"]

    def fetch(season):
        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
        params = {"stats": "gameLog", "group": "pitching", "season": season}
        try:
            r = requests.get(url, params=params, timeout=12).json()
            splits = r.get("stats", [{}])[0].get("splits", [])
            rows = []

            for g in splits:
                stat = g.get("stat", {})
                outs = int(stat.get("outs", 0))
                ip_string = stat.get("inningsPitched", "0.0")
                ip_float = innings_string_to_float(ip_string)

                rows.append({
                    "game_date": g.get("date"),
                    "wins": int(stat.get("wins", 0)),
                    "quality_starts": int(stat.get("qualityStarts", 0)),
                    "strikeouts": int(stat.get("strikeOuts", 0)),
                    "earned_runs": int(stat.get("earnedRuns", 0)),
                    "outs": outs,
                    "innings_pitched": ip_float,
                    "ip_display": ip_string,
                })
            return rows
        except:
            return []

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(fetch, seasons)

    all_rows = []
    for r in results:
        all_rows.extend(r)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df = df.dropna(subset=["game_date"])
    return df.sort_values("game_date", ascending=False)

# =========================================================
# CALCULATIONS
# =========================================================
def get_stat(row, stat_choice):
    stat_map = {
        "Hits": "hits",
        "Singles": "singles",
        "Home Runs": "home_runs",
        "Rbi": "rbi",
        "Runs": "runs",
        "Walks": "walks",
        "Strikeouts": "strikeouts",
        "Hits + Runs + Rbi": "hits_runs_rbi",
    }
    key = stat_map.get(stat_choice, stat_choice.lower())
    return row.get(key, 0)

def get_hitter_fantasy_score(row, book_name):
    s = FANTASY_SCORING[book_name]["Hitter"]
    return (
        row.get("singles", 0) * s["single"]
        + row.get("doubles", 0) * s["double"]
        + row.get("triples", 0) * s["triple"]
        + row.get("home_runs", 0) * s["home_run"]
        + row.get("runs", 0) * s["run"]
        + row.get("rbi", 0) * s["rbi"]
        + row.get("walks", 0) * s["walk"]
        + row.get("hbp", 0) * s["hbp"]
        + row.get("stolen_bases", 0) * s["stolen_base"]
    )

def get_pitcher_fantasy_score(row, book_name):
    s = FANTASY_SCORING[book_name]["Pitcher"]
    if book_name == "PrizePicks":
        return (
            row.get("wins", 0) * s["win"]
            + row.get("quality_starts", 0) * s["quality_start"]
            + row.get("earned_runs", 0) * s["earned_run"]
            + row.get("strikeouts", 0) * s["strikeout"]
            + row.get("outs", 0) * s["out"]
        )
    return (
        row.get("wins", 0) * s["win"]
        + row.get("quality_starts", 0) * s["quality_start"]
        + row.get("strikeouts", 0) * s["strikeout"]
        + row.get("innings_pitched", 0) * s["inning_pitched"]
        + row.get("earned_runs", 0) * s["earned_run"]
    )

def compute_current_streak(series_bool):
    current = 0
    for v in series_bool:
        if bool(v):
            current += 1
        else:
            break
    return current

def build_top_streaks(game_stats):
    gs = game_stats.sort_values("game_date")
    dates = gs["game_date"].tolist()
    cleared = gs["cleared"].tolist()

    streaks = []
    temp = 0
    start = None

    for i2 in range(len(cleared)):
        if cleared[i2]:
            if temp == 0:
                start = dates[i2]
            temp += 1
        else:
            if temp > 0:
                streaks.append((temp, start, dates[i2 - 1]))
            temp = 0

    if temp > 0:
        streaks.append((temp, start, dates[-1]))

    return sorted(streaks, key=lambda x: x[0], reverse=True)[:10]

def build_standard_game_stats(df, stat_choice, line, direction):
    df = df.copy()
    df["stat"] = df.apply(lambda row: get_stat(row, stat_choice), axis=1)
    game_stats = df[["game_date", "stat", "ab"]].copy()
    game_stats = game_stats.sort_values("game_date", ascending=False)

    if direction == "Over":
        game_stats["cleared"] = game_stats["stat"] >= float(line)
    else:
        game_stats["cleared"] = game_stats["stat"] <= float(line)

    game_stats, current, past = build_result_streaks(game_stats)
    label, prob = streak_rating(current, past)
    return game_stats, current, past, label, prob

def build_fantasy_game_stats(df, line, direction, book_name, role):
    df = df.copy()

    if role == "Pitcher":
        df["stat"] = df.apply(lambda row: get_pitcher_fantasy_score(row, book_name), axis=1)
        game_stats = df[["game_date", "stat", "outs", "ip_display"]].copy()
    else:
        df["stat"] = df.apply(lambda row: get_hitter_fantasy_score(row, book_name), axis=1)
        game_stats = df[["game_date", "stat", "ab"]].copy()

    game_stats = game_stats.sort_values("game_date", ascending=False)

    if direction == "Over":
        game_stats["cleared"] = game_stats["stat"] >= float(line)
    else:
        game_stats["cleared"] = game_stats["stat"] <= float(line)

    game_stats, current, past = build_result_streaks(game_stats)
    label, prob = streak_rating(current, past)
    return game_stats, current, past, label, prob

def get_standard_streaks_for_line(player_name, stat_choice, line):
    player_id = get_player_id(player_name)
    if not player_id:
        return {
            "player_name": player_name,
            "player_id": None,
            "not_found": True,
            "error_tag": MLB_LOOKUP_ERROR_TAG
        }

    df = load_hitter_data(player_id)
    if df.empty:
        return None

    gs = df.sort_values("game_date", ascending=False).copy()
    gs["stat"] = gs.apply(lambda row: get_stat(row, stat_choice), axis=1)
    gs["over_hit"] = gs["stat"] >= float(line)
    gs["under_hit"] = gs["stat"] <= float(line)

    over_current = compute_current_streak(gs["over_hit"].tolist())
    under_current = compute_current_streak(gs["under_hit"].tolist())

    return {
        "player_name": player_name,
        "player_id": player_id,
        "stat_choice": stat_choice,
        "line": line,
        "over_current": over_current,
        "under_current": under_current,
        "game_stats": gs
    }

def get_fantasy_streaks_for_line(player_name, line, book_name, role="Hitter"):
    player_id = get_player_id(player_name)
    if not player_id:
        return {
            "player_name": player_name,
            "player_id": None,
            "not_found": True,
            "error_tag": MLB_LOOKUP_ERROR_TAG if role in ["Hitter", "Pitcher"] else SPORTSBOOK_ERROR_TAG
        }

    df = load_pitcher_data(player_id) if role == "Pitcher" else load_hitter_data(player_id)
    if df.empty:
        return None

    gs = df.sort_values("game_date", ascending=False).copy()

    if role == "Pitcher":
        gs["stat"] = gs.apply(lambda row: get_pitcher_fantasy_score(row, book_name), axis=1)
    else:
        gs["stat"] = gs.apply(lambda row: get_hitter_fantasy_score(row, book_name), axis=1)

    gs["over_hit"] = gs["stat"] >= float(line)
    gs["under_hit"] = gs["stat"] <= float(line)

    over_current = compute_current_streak(gs["over_hit"].tolist())
    under_current = compute_current_streak(gs["under_hit"].tolist())

    return {
        "player_name": player_name,
        "player_id": player_id,
        "line": line,
        "over_current": over_current,
        "under_current": under_current,
        "game_stats": gs,
        "book_name": book_name,
        "role": role
    }

# =========================================================
# PROCESSORS
# =========================================================
def process_player(player, stat_choice, line, direction):
    player_id = get_player_id(player)

    if not player_id:
        return {
            "player_id": None,
            "player_name": player,
            "not_found": True,
            "error_tag": MLB_LOOKUP_ERROR_TAG
        }

    df = load_hitter_data(player_id)
    if df.empty:
        return {
            "player_id": player_id,
            "player_name": player,
            "not_found": False,
            "no_data": True
        }

    game_stats, current, past, label, prob = build_standard_game_stats(df, stat_choice, line, direction)

    return {
        "player_id": player_id,
        "player_name": player,
        "game_stats": game_stats,
        "current": current,
        "label": label,
        "prob": prob,
        "past": past,
        "not_found": False,
        "no_data": False
    }

def process_fantasy_player(player, line, direction, book_name, role):
    player_id = get_player_id(player)

    if not player_id:
        return {
            "player_id": None,
            "player_name": player,
            "not_found": True,
            "error_tag": MLB_LOOKUP_ERROR_TAG
        }

    df = load_pitcher_data(player_id) if role == "Pitcher" else load_hitter_data(player_id)
    if df.empty:
        return {
            "player_id": player_id,
            "player_name": player,
            "not_found": False,
            "no_data": True
        }

    game_stats, current, past, label, prob = build_fantasy_game_stats(df, line, direction, book_name, role)

    return {
        "player_id": player_id,
        "player_name": player,
        "game_stats": game_stats,
        "current": current,
        "label": label,
        "prob": prob,
        "past": past,
        "book_name": book_name,
        "role": role,
        "not_found": False,
        "no_data": False
    }

# =========================================================
# RENDERERS
# =========================================================
def render_not_found(player, error_tag):
    img = get_headshot(None)
    col1, col2 = st.columns([1, 6])
    with col1:
        st.image(img, width=60)
    with col2:
        st.markdown(
            f"""
            <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                <h3 style="margin:0;">{player}</h3>
                <span style="
                    background-color:#8b0000;
                    color:white;
                    padding:4px 10px;
                    border-radius:999px;
                    font-weight:700;
                    font-size:14px;
                ">
                    {error_tag}
                </span>
            </div>
            """,
            unsafe_allow_html=True
        )

def render_manual_player(player, r, line, direction):
    if r.get("not_found"):
        render_not_found(player, r.get("error_tag", MLB_LOOKUP_ERROR_TAG))
        return

    if r.get("no_data"):
        st.warning(f"{player} found, but no game log data was returned.")
        return

    img = get_headshot(r["player_id"])
    streak_color = "green" if r["current"] >= 3 else "white"

    col1, col2 = st.columns([1, 6])
    with col1:
        st.image(img, width=60)
        st.markdown(
            "<div style='color:red; font-weight:700; text-align:center; margin-top:4px;'>Victim Found</div>",
            unsafe_allow_html=True
        )
    with col2:
        st.markdown(
            f"### {player} — <span style='color:{streak_color}'>{r['current']}</span> — {r['label']}",
            unsafe_allow_html=True
        )

    with st.expander("View Details"):
        current_color = "green" if r["current"] >= 3 else "white"
        st.markdown(
            f"### 🔥 Top 10 Streaks (Current: <span style='color:{current_color}'>{r['current']}</span>)",
            unsafe_allow_html=True
        )

        top_10 = build_top_streaks(r["game_stats"])
        for s, start, end in top_10:
            color = "red" if s > r["current"] else "green" if s < r["current"] else "lightgray"
            st.markdown(
                f"<span style='color:{color}'>{s} game streak</span> ({start.date()} → {end.date()})",
                unsafe_allow_html=True
            )

        st.subheader("📊 Game Log")
        display = r["game_stats"].copy()
        display["result"] = display.apply(
            lambda row: f"{'🟢' if row['cleared'] else '❌'} {row['streak']}",
            axis=1
        )
        display = display[["game_date", "result", "stat", "ab"]]
        st.dataframe(display, use_container_width=True)

        df = r["game_stats"].copy()
        df = df.sort_values("game_date", ascending=True).tail(100).reset_index(drop=True)
        df["index"] = range(len(df))

        if direction == "Over":
            df["color"] = df["stat"].apply(lambda x: "green" if x >= float(line) else "red")
        else:
            df["color"] = df["stat"].apply(lambda x: "green" if x <= float(line) else "red")

        zoom = alt.selection_interval(bind="scales")
        bar = alt.Chart(df).mark_bar(size=4).encode(
            x=alt.X("index:Q", title="Last 100 Games"),
            y="stat:Q",
            color=alt.Color("color:N", scale=None),
            tooltip=["game_date", "stat", "streak"]
        ).add_selection(zoom)

        zero_text = alt.Chart(df).mark_text(dy=-10).encode(
            x="index:Q",
            y="stat:Q",
            text=alt.condition("datum.stat == 0", alt.value("0"), alt.value(""))
        )

        st.altair_chart(bar + zero_text, use_container_width=True)

def render_fantasy_player(player, r, line, direction):
    if r.get("not_found"):
        render_not_found(player, r.get("error_tag", MLB_LOOKUP_ERROR_TAG))
        return

    if r.get("no_data"):
        st.warning(f"{player} found, but no game log data was returned.")
        return

    img = get_headshot(r["player_id"])
    streak_color = "green" if r["current"] >= 3 else "white"

    col1, col2 = st.columns([1, 6])
    with col1:
        st.image(img, width=60)
        st.markdown(
            "<div style='color:red; font-weight:700; text-align:center; margin-top:4px;'>Victim Found</div>",
            unsafe_allow_html=True
        )
    with col2:
        st.markdown(
            f"### {player} — <span style='color:{streak_color}'>{r['current']}</span> — {r['label']}",
            unsafe_allow_html=True
        )
        st.markdown(
            f"<div style='color:#cccccc; margin-top:-8px;'>{r['book_name']} {r['role']} Fantasy Score</div>",
            unsafe_allow_html=True
        )

    with st.expander("View Details"):
        current_color = "green" if r["current"] >= 3 else "white"
        st.markdown(
            f"### 🔥 Top 10 Streaks (Current: <span style='color:{current_color}'>{r['current']}</span>)",
            unsafe_allow_html=True
        )

        top_10 = build_top_streaks(r["game_stats"])
        for s, start, end in top_10:
            color = "red" if s > r["current"] else "green" if s < r["current"] else "lightgray"
            st.markdown(
                f"<span style='color:{color}'>{s} game streak</span> ({start.date()} → {end.date()})",
                unsafe_allow_html=True
            )

        st.subheader("📊 Game Log")
        display = r["game_stats"].copy()
        display["result"] = display.apply(
            lambda row: f"{'🟢' if row['cleared'] else '❌'} {row['streak']}",
            axis=1
        )
        if r["role"] == "Pitcher":
            display = display[["game_date", "result", "stat", "ip_display", "outs"]]
        else:
            display = display[["game_date", "result", "stat", "ab"]]
        st.dataframe(display, use_container_width=True)

        df = r["game_stats"].copy()
        df = df.sort_values("game_date", ascending=True).tail(100).reset_index(drop=True)
        df["index"] = range(len(df))

        if direction == "Over":
            df["color"] = df["stat"].apply(lambda x: "green" if x >= float(line) else "red")
        else:
            df["color"] = df["stat"].apply(lambda x: "green" if x <= float(line) else "red")

        zoom = alt.selection_interval(bind="scales")
        bar = alt.Chart(df).mark_bar(size=4).encode(
            x=alt.X("index:Q", title="Last 100 Games"),
            y="stat:Q",
            color=alt.Color("color:N", scale=None),
            tooltip=["game_date", "stat", "streak"]
        ).add_selection(zoom)

        zero_text = alt.Chart(df).mark_text(dy=-10).encode(
            x="index:Q",
            y="stat:Q",
            text=alt.condition("datum.stat == 0", alt.value("0"), alt.value(""))
        )

        st.altair_chart(bar + zero_text, use_container_width=True)

def build_scan_display_game_log(base_df, qualified_stats):
    base_sorted = base_df.sort_values("game_date", ascending=False).reset_index(drop=True)
    display = pd.DataFrame({"game_date": base_sorted["game_date"]})

    if "ab" in base_sorted.columns:
        display["ab"] = base_sorted["ab"]

    for idx, q in enumerate(qualified_stats, start=1):
        df = base_sorted.copy()

        if q["Mode"] == "fantasy":
            df["stat"] = df.apply(lambda row: get_hitter_fantasy_score(row, q["Fantasy Book"]), axis=1)
        else:
            df["stat"] = df.apply(lambda row: get_stat(row, q["Stat"]), axis=1)

        if q["Direction"] == "Over":
            df["cleared"] = df["stat"] >= float(q["Line"])
        else:
            df["cleared"] = df["stat"] <= float(q["Line"])

        streak = 0
        streak_list = []
        for v in df["cleared"][::-1]:
            streak = streak + 1 if v else 0
            streak_list.append(streak)
        df["streak"] = list(reversed(streak_list))

        result_col = f"result_{idx}"
        stat_col = f"stat_{idx}"

        if q["Mode"] == "fantasy":
            display[result_col] = df.apply(
                lambda row: f"{q['Fantasy Book']} FP {'🟢' if row['cleared'] else '❌'} {row['streak']}",
                axis=1
            ).values
        else:
            display[result_col] = df.apply(
                lambda row: f"{'🟢' if row['cleared'] else '❌'} {row['streak']}",
                axis=1
            ).values

        display[stat_col] = df["stat"].values

    return display

def build_scan_top_streaks(base_df, q):
    df = base_df.copy()

    if q["Mode"] == "fantasy":
        df["stat"] = df.apply(lambda row: get_hitter_fantasy_score(row, q["Fantasy Book"]), axis=1)
    else:
        df["stat"] = df.apply(lambda row: get_stat(row, q["Stat"]), axis=1)

    keep_cols = ["game_date", "stat", "ab"]
    df = df[keep_cols].copy()
    df = df.sort_values("game_date", ascending=False)

    if q["Direction"] == "Over":
        df["cleared"] = df["stat"] >= float(q["Line"])
    else:
        df["cleared"] = df["stat"] <= float(q["Line"])

    current = 0
    for v in df["cleared"]:
        if v:
            current += 1
        else:
            break

    return df, current, build_top_streaks(df)

def render_scan_player_card(player_name, player_id, team, matchup, book_title, qualified_stats, base_df):
    img = get_headshot(player_id)
    biggest_current = max(q["Current"] for q in qualified_stats) if qualified_stats else 0
    streak_color = "green" if biggest_current >= 3 else "white"

    col1, col2 = st.columns([1, 6])
    with col1:
        st.image(img, width=60)
        st.markdown(
            "<div style='color:red; font-weight:700; text-align:center; margin-top:4px;'>Victim Found</div>",
            unsafe_allow_html=True
        )
    with col2:
        title_parts = []
        for q in qualified_stats:
            if q["Mode"] == "fantasy":
                title_parts.append(f"{q['Fantasy Book']} Fantasy {q['Line']} {q['Direction']} ({q['Current']})")
            else:
                title_parts.append(f"{q['Stat']} {q['Line']} {q['Direction']} ({q['Current']})")

        joined_title = " | ".join(title_parts)
        st.markdown(
            f"### {player_name} — <span style='color:{streak_color}'>{biggest_current}</span> — {team} — {matchup} — {book_title}",
            unsafe_allow_html=True
        )
        st.markdown(f"<div style='color:#cccccc; margin-top:-8px;'>{joined_title}</div>", unsafe_allow_html=True)

    with st.expander("View Details"):
        st.markdown("### 🔥 Top 10 Streaks", unsafe_allow_html=True)

        for q in qualified_stats:
            label_txt = f"{q['Fantasy Book']} Fantasy Points" if q["Mode"] == "fantasy" else q["Stat"]
            st.markdown(
                f"#### {label_txt} — Line {q['Line']} — {q['Direction']} (Current: <span style='color:{'green' if q['Current'] >= 3 else 'white'}'>{q['Current']}</span>)",
                unsafe_allow_html=True
            )
            _, current, top_10 = build_scan_top_streaks(base_df, q)

            for s, start, end in top_10:
                color = "red" if s > current else "green" if s < current else "lightgray"
                st.markdown(
                    f"<span style='color:{color}'>{s} game streak</span> ({start.date()} → {end.date()})",
                    unsafe_allow_html=True
                )

        st.subheader("📊 Game Log")
        display = build_scan_display_game_log(base_df, qualified_stats)
        st.dataframe(display, use_container_width=True)

        st.subheader("📉 Last 100 Games Charts")
        for q in qualified_stats:
            df = base_df.copy()

            if q["Mode"] == "fantasy":
                chart_title = f"{q['Fantasy Book']} Fantasy Points — Line {q['Line']} — {q['Direction']}"
                df["stat"] = df.apply(lambda row: get_hitter_fantasy_score(row, q["Fantasy Book"]), axis=1)
            else:
                chart_title = f"{q['Stat']} — Line {q['Line']} — {q['Direction']}"
                df["stat"] = df.apply(lambda row: get_stat(row, q["Stat"]), axis=1)

            st.markdown(f"#### {chart_title}")
            df = df[["game_date", "stat"]].copy()
            df = df.sort_values("game_date", ascending=True).tail(100).reset_index(drop=True)
            df["index"] = range(len(df))

            if q["Direction"] == "Over":
                df["color"] = df["stat"].apply(lambda x: "green" if x >= float(q["Line"]) else "red")
            else:
                df["color"] = df["stat"].apply(lambda x: "green" if x <= float(q["Line"]) else "red")

            zoom = alt.selection_interval(bind="scales")
            bar = alt.Chart(df).mark_bar(size=4).encode(
                x=alt.X("index:Q", title="Last 100 Games"),
                y="stat:Q",
                color=alt.Color("color:N", scale=None),
                tooltip=["game_date", "stat"]
            ).add_selection(zoom)

            zero_text = alt.Chart(df).mark_text(dy=-10).encode(
                x="index:Q",
                y="stat:Q",
                text=alt.condition("datum.stat == 0", alt.value("0"), alt.value(""))
            )

            st.altair_chart(bar + zero_text, use_container_width=True)

# =========================================================
# MLB TODAY STARTERS
# =========================================================
@st.cache_data(show_spinner=False, ttl=60 * 10)
def get_today_games():
    url = "https://statsapi.mlb.com/api/v1/schedule"
    params = {"sportId": 1, "date": TODAY}
    try:
        r = requests.get(url, params=params, timeout=15).json()
        games = r.get("dates", [])
        if not games:
            return []
        return games[0].get("games", [])
    except:
        return []

@st.cache_data(show_spinner=False, ttl=60 * 10)
def get_game_boxscore(game_pk):
    url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
    try:
        return requests.get(url, timeout=15).json()
    except:
        return {}

def parse_lineup_players(boxscore_json):
    starters = []

    teams = boxscore_json.get("teams", {})
    for side in ["home", "away"]:
        team_data = teams.get(side, {})
        players = team_data.get("players", {})

        team_name = ""
        team_info = team_data.get("team", {})
        if isinstance(team_info, dict):
            team_name = team_info.get("name", "")

        for _, pdata in players.items():
            person = pdata.get("person", {})
            full_name = person.get("fullName", "")
            batting_order = pdata.get("battingOrder", "")
            position = pdata.get("position", {}).get("abbreviation", "")

            if full_name and batting_order:
                starters.append({
                    "player_name": full_name,
                    "normalized_name": normalize_name(full_name),
                    "team": team_name,
                    "batting_order": batting_order,
                    "position": position,
                })

    return starters

def get_today_starting_lineups():
    games = get_today_games()
    all_starters = []

    for g in games:
        game_pk = g.get("gamePk")
        if not game_pk:
            continue

        box = get_game_boxscore(game_pk)
        starters = parse_lineup_players(box)

        home_name = g.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
        away_name = g.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
        matchup = f"{away_name} @ {home_name}"

        for s in starters:
            s["matchup"] = matchup
            s["game_pk"] = game_pk
            all_starters.append(s)

    if not all_starters:
        return pd.DataFrame(columns=["player_name", "normalized_name", "team", "batting_order", "position", "matchup", "game_pk"])

    df = pd.DataFrame(all_starters)
    df = df.drop_duplicates(subset=["normalized_name", "game_pk"])
    return df

# =========================================================
# ODDS API / CACHE
# =========================================================
def get_cached_props(bookmaker_key, scan_date):
    q = """
    SELECT scan_date, bookmaker_key, event_id, market_key, player_name, line, over_price, under_price, last_update
    FROM props_cache
    WHERE scan_date = ? AND bookmaker_key = ?
    """
    return pd.read_sql_query(q, conn, params=(scan_date, bookmaker_key))

def get_cache_meta(bookmaker_key, scan_date):
    cursor.execute("""
        SELECT refreshed_at FROM props_cache_meta
        WHERE scan_date = ? AND bookmaker_key = ?
    """, (scan_date, bookmaker_key))
    row = cursor.fetchone()
    return row[0] if row else None

def clear_props_cache(bookmaker_key, scan_date):
    cursor.execute("DELETE FROM props_cache WHERE scan_date = ? AND bookmaker_key = ?", (scan_date, bookmaker_key))
    cursor.execute("DELETE FROM props_cache_meta WHERE scan_date = ? AND bookmaker_key = ?", (scan_date, bookmaker_key))
    conn.commit()

def save_props_cache(bookmaker_key, scan_date, props_rows):
    clear_props_cache(bookmaker_key, scan_date)

    for row in props_rows:
        cursor.execute("""
            INSERT OR REPLACE INTO props_cache
            (scan_date, bookmaker_key, event_id, market_key, player_name, line, over_price, under_price, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            scan_date,
            bookmaker_key,
            row.get("event_id"),
            row.get("market_key"),
            row.get("player_name"),
            row.get("line"),
            row.get("over_price"),
            row.get("under_price"),
            row.get("last_update"),
        ))

    cursor.execute("""
        INSERT OR REPLACE INTO props_cache_meta (scan_date, bookmaker_key, refreshed_at)
        VALUES (?, ?, ?)
    """, (scan_date, bookmaker_key, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    conn.commit()

@st.cache_data(show_spinner=False, ttl=60 * 5)
def odds_get_today_events():
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us_dfs",
        "markets": "h2h",
        "oddsFormat": "american",
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return [], {}
        headers = {
            "x-requests-used": r.headers.get("x-requests-used", ""),
            "x-requests-remaining": r.headers.get("x-requests-remaining", ""),
            "x-requests-last": r.headers.get("x-requests-last", "")
        }
        return r.json(), headers
    except:
        return [], {}

def parse_event_prop_response(event_json, event_id):
    rows = []
    bookmakers = event_json.get("bookmakers", [])
    for book in bookmakers:
        book_last = book.get("last_update", "")
        markets = book.get("markets", [])
        for market in markets:
            market_key = market.get("key")
            outcomes = market.get("outcomes", [])
            temp = {}

            for out in outcomes:
                player_name = out.get("description") or out.get("player") or out.get("name")
                side = str(out.get("name", "")).strip().lower()
                point = out.get("point")

                if side not in ["over", "under"]:
                    continue
                if not player_name or point is None:
                    continue

                k = (player_name, market_key, float(point))
                if k not in temp:
                    temp[k] = {
                        "player_name": player_name,
                        "market_key": market_key,
                        "line": float(point),
                        "over_price": None,
                        "under_price": None,
                        "last_update": market.get("last_update") or book_last,
                        "event_id": event_id,
                    }

                if side == "over":
                    temp[k]["over_price"] = out.get("price")
                elif side == "under":
                    temp[k]["under_price"] = out.get("price")

            rows.extend(temp.values())
    return rows

def fetch_props_from_odds_api(bookmaker_key):
    events, usage_headers = odds_get_today_events()
    if not events:
        return [], usage_headers

    market_list = ",".join(SUPPORTED_PROP_MARKETS.values())
    all_rows = []
    last_headers = usage_headers.copy()

    for ev in events:
        event_id = ev.get("id")
        if not event_id:
            continue

        url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{event_id}/odds"
        params = {
            "apiKey": ODDS_API_KEY,
            "bookmakers": bookmaker_key,
            "markets": market_list,
            "oddsFormat": "american",
        }

        try:
            r = requests.get(url, params=params, timeout=25)
            if r.status_code != 200:
                continue

            last_headers = {
                "x-requests-used": r.headers.get("x-requests-used", last_headers.get("x-requests-used", "")),
                "x-requests-remaining": r.headers.get("x-requests-remaining", last_headers.get("x-requests-remaining", "")),
                "x-requests-last": r.headers.get("x-requests-last", last_headers.get("x-requests-last", "")),
            }
            all_rows.extend(parse_event_prop_response(r.json(), event_id))
        except:
            continue

    return all_rows, last_headers

def get_props_for_book(bookmaker_key, force_refresh=False):
    if force_refresh:
        clear_props_cache(bookmaker_key, TODAY)

    cached = get_cached_props(bookmaker_key, TODAY)
    if not cached.empty and not force_refresh:
        return cached, True, get_cache_meta(bookmaker_key, TODAY), {}

    rows, usage_headers = fetch_props_from_odds_api(bookmaker_key)
    if rows:
        save_props_cache(bookmaker_key, TODAY, rows)
        cached = get_cached_props(bookmaker_key, TODAY)
        return cached, False, get_cache_meta(bookmaker_key, TODAY), usage_headers

    return pd.DataFrame(), False, None, usage_headers

def market_key_to_stat(market_key):
    reverse = {v: k for k, v in SUPPORTED_PROP_MARKETS.items()}
    return reverse.get(market_key)

def clear_scan_results_cache(bookmaker_key, scan_date):
    cursor.execute("DELETE FROM scan_results_players WHERE scan_date = ? AND bookmaker_key = ?", (scan_date, bookmaker_key))
    cursor.execute("DELETE FROM scan_results_stats WHERE scan_date = ? AND bookmaker_key = ?", (scan_date, bookmaker_key))
    cursor.execute("DELETE FROM scan_results_meta WHERE scan_date = ? AND bookmaker_key = ?", (scan_date, bookmaker_key))
    conn.commit()

def save_scan_results(bookmaker_key, scan_date, refreshed_at, display_players):
    clear_scan_results_cache(bookmaker_key, scan_date)

    for pdata in display_players:
        normalized_name = normalize_name(pdata["Player"])
        cursor.execute("""
            INSERT OR REPLACE INTO scan_results_players
            (scan_date, bookmaker_key, normalized_name, player_name, team, matchup, book, player_id, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            scan_date,
            bookmaker_key,
            normalized_name,
            pdata["Player"],
            pdata["Team"],
            pdata["Matchup"],
            pdata["Book"],
            pdata["Player ID"],
            pdata["Error"],
        ))

        for idx, q in enumerate(pdata.get("Qualified Stats", []), start=1):
            cursor.execute("""
                INSERT OR REPLACE INTO scan_results_stats
                (scan_date, bookmaker_key, normalized_name, stat_index, stat, line, direction, current, mode, fantasy_book)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                scan_date,
                bookmaker_key,
                normalized_name,
                idx,
                q.get("Stat"),
                q.get("Line"),
                q.get("Direction"),
                q.get("Current"),
                q.get("Mode"),
                q.get("Fantasy Book"),
            ))

    cursor.execute("""
        INSERT OR REPLACE INTO scan_results_meta (scan_date, bookmaker_key, refreshed_at, result_count)
        VALUES (?, ?, ?, ?)
    """, (scan_date, bookmaker_key, refreshed_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(display_players)))

    conn.commit()

def get_saved_scan_results(bookmaker_key, scan_date):
    players_df = pd.read_sql_query("""
        SELECT normalized_name, player_name, team, matchup, book, player_id, error
        FROM scan_results_players
        WHERE scan_date = ? AND bookmaker_key = ?
        ORDER BY player_name
    """, conn, params=(scan_date, bookmaker_key))

    stats_df = pd.read_sql_query("""
        SELECT normalized_name, stat_index, stat, line, direction, current, mode, fantasy_book
        FROM scan_results_stats
        WHERE scan_date = ? AND bookmaker_key = ?
        ORDER BY normalized_name, stat_index
    """, conn, params=(scan_date, bookmaker_key))

    cursor.execute("""
        SELECT refreshed_at, result_count FROM scan_results_meta
        WHERE scan_date = ? AND bookmaker_key = ?
    """, (scan_date, bookmaker_key))
    meta_row = cursor.fetchone()

    if players_df.empty and not meta_row:
        return [], None, None

    display_players = []
    for row in players_df.itertuples(index=False):
        pstats = []
        if not stats_df.empty:
            sub = stats_df[stats_df["normalized_name"] == row.normalized_name]
            for s in sub.itertuples(index=False):
                pstats.append({
                    "Stat": s.stat,
                    "Line": float(s.line),
                    "Direction": s.direction,
                    "Current": int(s.current),
                    "Mode": s.mode,
                    "Fantasy Book": s.fantasy_book,
                })

        display_players.append({
            "Player": row.player_name,
            "Team": row.team,
            "Matchup": row.matchup,
            "Book": row.book,
            "Player ID": None if pd.isna(row.player_id) else int(row.player_id),
            "Error": row.error or "",
            "Qualified Stats": pstats,
        })

    refreshed_at = meta_row[0] if meta_row else None
    result_count = int(meta_row[1]) if meta_row and meta_row[1] is not None else len(display_players)
    return display_players, refreshed_at, result_count

# =========================================================
# PAGE LAYOUT
# =========================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "Manual Analyzer",
    "All Stat Streaks",
    "Daily Starters Props Scan",
    "Fantasy Score Analyzer"
])

# =========================================================
# TAB 1 - MANUAL ANALYZER
# =========================================================
with tab1:
    blocks = []

    for i in range(1, 4):
        st.markdown(f"## 📊 Block {i}")

        players_input = st.text_area("Players (one per line)", key=f"p{i}")

        hits_stat = st.selectbox("Hits Group", HITS_GROUP, key=f"h{i}")
        use_hits = st.checkbox("Use Hits", key=f"uh{i}")
        hits_dir = st.radio("Hits Line Type", ["Over", "Under"], key=f"hd{i}")

        other_stat = st.selectbox("Other", ["Walks", "Strikeouts"], key=f"o{i}")
        use_other = st.checkbox("Use Other", key=f"uo{i}")
        other_dir = st.radio("Other Line Type", ["Over", "Under"], key=f"od{i}")

        stat_choice = hits_stat if use_hits else other_stat
        direction = hits_dir if use_hits else other_dir

        line = st.number_input("Line", 0.0, step=0.5, value=0.5, key=f"l{i}")

        players = [p.strip() for p in players_input.split("\n") if p.strip()]
        blocks.append((players, stat_choice, line, direction))

    if st.button("RUN ANALYSIS"):
        for i, (players, stat_choice, line, direction) in enumerate(blocks, 1):
            st.markdown(f"# 📊 Block {i}")
            for player in players:
                r = process_player(player, stat_choice, line, direction)
                render_manual_player(player, r, line, direction)

# =========================================================
# TAB 2 - ALL STAT STREAKS
# =========================================================
with tab2:
    st.subheader("📚 Analyze All Stat Streaks For One Player")
    all_stat_player = st.text_input("Player Name", key="all_stat_player")
    all_stat_line = st.number_input("Default line for all stats", min_value=0.0, step=0.5, value=0.5, key="all_stat_line")

    if st.button("RUN ALL STAT STREAKS"):
        if not all_stat_player.strip():
            st.warning("Enter a player name.")
        else:
            pid = get_player_id(all_stat_player.strip())
            if not pid:
                render_not_found(all_stat_player.strip(), MLB_LOOKUP_ERROR_TAG)
            else:
                df = load_hitter_data(pid)
                if df.empty:
                    st.warning("No data found for that player.")
                else:
                    img = get_headshot(pid)
                    col1, col2 = st.columns([1, 6])
                    with col1:
                        st.image(img, width=75)
                        st.markdown(
                            "<div style='color:red; font-weight:700; text-align:center; margin-top:4px;'>Victim Found</div>",
                            unsafe_allow_html=True
                        )
                    with col2:
                        st.markdown(f"## {all_stat_player.strip()}")

                    rows = []
                    for stat_name in ALL_STATS_SCAN:
                        _, current_over, _, _, _ = build_standard_game_stats(df, stat_name, all_stat_line, "Over")
                        _, current_under, _, _, _ = build_standard_game_stats(df, stat_name, all_stat_line, "Under")
                        rows.append({
                            "Stat": stat_name,
                            "Line": all_stat_line,
                            "Over Streak": current_over,
                            "Under Streak": current_under
                        })

                    out = pd.DataFrame(rows)
                    st.dataframe(out, use_container_width=True)

# =========================================================
# TAB 3 - DAILY STARTERS PROPS SCAN
# =========================================================
with tab3:
    st.subheader("📅 Today’s Starting Lineups + Props Streak Scan")

    pp = st.checkbox("PrizePicks", key="book_pp")
    ud = st.checkbox("Underdog", key="book_ud")

    selected = []
    if pp:
        selected.append("PrizePicks")
    if ud:
        selected.append("Underdog")

    if len(selected) > 1:
        st.error("Pick only one sportsbook at a time.")
    elif len(selected) == 0:
        st.info("Choose PrizePicks or Underdog.")
    else:
        book_title = selected[0]
        bookmaker_key = BOOKMAKER_MAP[book_title]

        col_a, col_b, col_c = st.columns(3)
        with col_a:
            run_scan = st.button("Pull Today's Starting Players + Scan Props", key="run_daily_scan")
        with col_b:
            show_last_scan = st.button("Show Last Scan", key="show_last_scan")
        with col_c:
            refresh_scan = st.button("Refresh Saved Lines From Sportsbook", key="refresh_daily_scan")

        if run_scan or show_last_scan or refresh_scan:
            force_refresh = bool(refresh_scan)
            cache_only = bool(show_last_scan)
            display_players = []
            usage_headers = {}
            refreshed_at = None

            if cache_only:
                with st.spinner("Loading last saved scan..."):
                    starters_df = get_today_starting_lineups()
                    display_players, refreshed_at, saved_count = get_saved_scan_results(bookmaker_key, TODAY)

                if starters_df.empty:
                    st.warning("No starting lineups were found from MLB yet.")
                else:
                    st.write(f"Starting lineup players found: **{len(starters_df)}**")

                if refreshed_at:
                    st.caption(f"Using last saved scan. Last saved update: {refreshed_at}")
                else:
                    st.warning("No saved scan found for today yet. Run a fresh scan first.")

                if refreshed_at and not display_players and saved_count == 0:
                    st.info("Last saved scan found no starting players with a 3+ streak against the current lines.")

            else:
                with st.spinner("Loading today's starters and prop lines..."):
                    starters_df = get_today_starting_lineups()
                    props_df, from_cache, refreshed_at, usage_headers = get_props_for_book(
                        bookmaker_key,
                        force_refresh=force_refresh
                    )

                if starters_df.empty:
                    st.warning("No starting lineups were found from MLB yet.")
                else:
                    st.write(f"Starting lineup players found: **{len(starters_df)}**")

                if usage_headers:
                    used = usage_headers.get("x-requests-used", "")
                    remaining = usage_headers.get("x-requests-remaining", "")
                    last = usage_headers.get("x-requests-last", "")
                    st.caption(f"Odds API usage — Used: {used} | Remaining: {remaining} | Last request cost: {last}")

                if props_df.empty:
                    st.warning("No props came back from the sportsbook/API for today.")
                else:
                    source_txt = "saved cache" if from_cache else "fresh sportsbook request"
                    meta_txt = f"Last saved update: {refreshed_at}" if refreshed_at else "No saved timestamp yet"
                    st.caption(f"Using {source_txt}. {meta_txt}")

                    props_df["normalized_name"] = props_df["player_name"].apply(normalize_name)

                    if starters_df.empty:
                        merged = props_df.iloc[0:0].copy()
                    else:
                        merged = props_df.merge(
                            starters_df[["player_name", "normalized_name", "team", "position", "matchup"]],
                            on="normalized_name",
                            how="inner",
                            suffixes=("_prop", "_starter")
                        )

                    if merged.empty:
                        st.warning("No sportsbook props matched the currently posted starting lineups.")
                        save_scan_results(bookmaker_key, TODAY, refreshed_at, [])
                    else:
                        merged = merged.drop_duplicates(subset=["normalized_name", "market_key", "line"]).copy()

                        player_groups = {}
                        prog = st.progress(0)
                        total = len(merged)

                        for idx, row in enumerate(merged.itertuples(index=False), start=1):
                            stat_choice = market_key_to_stat(row.market_key)
                            if not stat_choice:
                                prog.progress(min(idx / total, 1.0))
                                continue

                            player_key = normalize_name(row.player_name_starter)

                            if player_key not in player_groups:
                                player_groups[player_key] = {
                                    "Player": row.player_name_starter,
                                    "Team": row.team,
                                    "Matchup": row.matchup,
                                    "Book": book_title,
                                    "Player ID": None,
                                    "Error": "",
                                    "Qualified Stats": []
                                }

                            if stat_choice == "Fantasy Points":
                                streak_data = get_fantasy_streaks_for_line(
                                    player_name=row.player_name_starter,
                                    line=float(row.line),
                                    book_name=book_title,
                                    role="Hitter"
                                )
                            else:
                                streak_data = get_standard_streaks_for_line(
                                    player_name=row.player_name_starter,
                                    stat_choice=stat_choice,
                                    line=float(row.line)
                                )

                            if streak_data and streak_data.get("not_found"):
                                player_groups[player_key]["Error"] = SPORTSBOOK_ERROR_TAG
                                player_groups[player_key]["Player ID"] = None

                            elif streak_data:
                                player_groups[player_key]["Player ID"] = streak_data["player_id"]
                                over_s = streak_data["over_current"]
                                under_s = streak_data["under_current"]

                                if over_s >= 3:
                                    player_groups[player_key]["Qualified Stats"].append({
                                        "Stat": stat_choice,
                                        "Line": float(row.line),
                                        "Direction": "Over",
                                        "Current": over_s,
                                        "Mode": "fantasy" if stat_choice == "Fantasy Points" else "standard",
                                        "Fantasy Book": book_title if stat_choice == "Fantasy Points" else None,
                                    })

                                if under_s >= 3:
                                    player_groups[player_key]["Qualified Stats"].append({
                                        "Stat": stat_choice,
                                        "Line": float(row.line),
                                        "Direction": "Under",
                                        "Current": under_s,
                                        "Mode": "fantasy" if stat_choice == "Fantasy Points" else "standard",
                                        "Fantasy Book": book_title if stat_choice == "Fantasy Points" else None,
                                    })

                            prog.progress(min(idx / total, 1.0))

                        prog.empty()

                        for _, pdata in player_groups.items():
                            if pdata["Error"] or len(pdata["Qualified Stats"]) > 0:
                                display_players.append(pdata)

                        save_scan_results(bookmaker_key, TODAY, refreshed_at, display_players)

                        if not display_players:
                            st.info("No starting players had an over or under streak of 3+ against the current line.")

            if display_players:
                summary_rows = []
                for pdata in display_players:
                    if pdata["Error"]:
                        summary_rows.append({
                            "Player": pdata["Player"],
                            "Team": pdata["Team"],
                            "Matchup": pdata["Matchup"],
                            "Book": pdata["Book"],
                            "Qualified Stats Found": "N/A",
                            "Error": pdata["Error"]
                        })
                    else:
                        summary_rows.append({
                            "Player": pdata["Player"],
                            "Team": pdata["Team"],
                            "Matchup": pdata["Matchup"],
                            "Book": pdata["Book"],
                            "Qualified Stats Found": len(pdata["Qualified Stats"]),
                            "Error": ""
                        })

                summary_df = pd.DataFrame(summary_rows).sort_values(
                    by=["Qualified Stats Found", "Player"],
                    ascending=[False, True]
                ).reset_index(drop=True)

                st.markdown(f"## ✅ Players with at least one 3+ streak vs {book_title} lines")
                st.dataframe(summary_df, use_container_width=True)

                st.markdown("## Player Cards")
                for pdata in display_players:
                    if pdata["Error"]:
                        render_not_found(pdata["Player"], pdata["Error"])
                        st.markdown(
                            f"<div style='margin-top:4px; color:#cccccc;'>{pdata['Team']} • {pdata['Matchup']} • {pdata['Book']}</div>",
                            unsafe_allow_html=True
                        )
                    else:
                        player_id = pdata["Player ID"]
                        base_df = load_hitter_data(player_id)

                        if base_df.empty:
                            st.warning(f"{pdata['Player']} found, but no game log data was returned.")
                            continue

                        render_scan_player_card(
                            player_name=pdata["Player"],
                            player_id=player_id,
                            team=pdata["Team"],
                            matchup=pdata["Matchup"],
                            book_title=pdata["Book"],
                            qualified_stats=pdata["Qualified Stats"],
                            base_df=base_df
                        )

# =========================================================
# TAB 4 - FANTASY SCORE ANALYZER
# =========================================================
with tab4:
    st.subheader("💎 Fantasy Score Analyzer")
    st.caption("This page uses MLB game logs only. It does not use Odds API.")

    st.markdown("## 🎯 Manual Pitcher Fantasy Input")
    pitcher_players_input = st.text_area(
        "Pitcher names (one per line)",
        key="pitcher_fantasy_players"
    )

    pcol1, pcol2, pcol3 = st.columns(3)
    with pcol1:
        pitcher_fantasy_book = st.selectbox(
            "Fantasy Rules",
            ["PrizePicks", "Underdog"],
            key="pitcher_fantasy_book"
        )
    with pcol2:
        pitcher_fantasy_direction = st.radio(
            "Pitcher Line Type",
            ["Over", "Under"],
            key="pitcher_fantasy_direction"
        )
    with pcol3:
        pitcher_fantasy_line = st.number_input(
            "Pitcher Fantasy Line",
            min_value=0.0,
            step=0.5,
            value=18.5,
            key="pitcher_fantasy_line"
        )

    if st.button("RUN PITCHER FANTASY ANALYZER"):
        players = [p.strip() for p in pitcher_players_input.split("\n") if p.strip()]
        if not players:
            st.warning("Enter at least one pitcher.")
        else:
            for player in players:
                r = process_fantasy_player(
                    player=player,
                    line=pitcher_fantasy_line,
                    direction=pitcher_fantasy_direction,
                    book_name=pitcher_fantasy_book,
                    role="Pitcher"
                )
                render_fantasy_player(player, r, pitcher_fantasy_line, pitcher_fantasy_direction)

    st.markdown("---")
    st.markdown("## 📘 Hitter Fantasy Score Analyzer")

    fantasy_player_input = st.text_area(
        "Hitter names (one per line)",
        key="fantasy_players"
    )

    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        fantasy_book = st.selectbox("Fantasy Rules", ["PrizePicks", "Underdog"], key="fantasy_book")
    with fcol2:
        fantasy_direction = st.radio("Line Type", ["Over", "Under"], key="fantasy_direction")
    with fcol3:
        fantasy_line = st.number_input("Fantasy Line", min_value=0.0, step=0.5, value=6.5, key="fantasy_line")

    if st.button("RUN HITTER FANTASY ANALYZER"):
        players = [p.strip() for p in fantasy_player_input.split("\n") if p.strip()]
        if not players:
            st.warning("Enter at least one hitter.")
        else:
            for player in players:
                r = process_fantasy_player(
                    player=player,
                    line=fantasy_line,
                    direction=fantasy_direction,
                    book_name=fantasy_book,
                    role="Hitter"
                )
                render_fantasy_player(player, r, fantasy_line, fantasy_direction)