# src/visualisation.py
import matplotlib
matplotlib.use("Agg")   # mode headless (génère des PNG sans fenêtre)
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def fig1_courses_par_heure(q1: pd.DataFrame, save: bool = True):
    """Q1 — Courbe des courses par heure (0-23)."""
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(q1["heure"], q1["nb_courses"], marker="o", color="steelblue", lw=2)
    ax.set_xlabel("Heure de la journée")
    ax.set_ylabel("Nombre de courses")
    ax.set_title("Q1 — Distribution des courses par heure (NYC, jan-mar 2024)")
    ax.grid(True, alpha=0.3)
    ax.set_xticks(range(0, 24, 2))
    plt.tight_layout()
    if save:
        plt.savefig("output/graphiques/q1_courses_par_heure.png", dpi=100)
    plt.close()
    print("    → q1_courses_par_heure.png")


def fig2_top_zones(actives: pd.DataFrame, save: bool = True):
    """Q2 — Barres horizontales du top 10 zones actives."""
    fig, ax = plt.subplots(figsize=(10, 5))
    df = actives.sort_values("nb_courses", ascending=True)
    ax.barh(df["Zone"], df["nb_courses"], color="coral")
    ax.set_xlabel("Nombre de courses")
    ax.set_title("Q2 — Top 10 zones les plus actives (pickup)")
    plt.tight_layout()
    if save:
        plt.savefig("output/graphiques/q2_top_zones.png", dpi=100)
    plt.close()
    print("    → q2_top_zones.png")


def fig3_tips_par_borough(q3: pd.DataFrame, save: bool = True):
    """Q3 — Tip moyen par borough (exclut Unknown / N/A)."""
    df = q3[~q3["Borough"].isin(["Unknown", "N/A"])].copy()
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(df["Borough"], df["tip_moyen"] * 100, color="mediumseagreen")
    ax.set_ylabel("Tip moyen (%)")
    ax.set_title("Q3 — Tip moyen par borough de départ")
    ax.axhline(y=df["tip_moyen"].mean() * 100, color="red",
               linestyle="--", label="Moyenne globale")
    ax.legend()
    plt.tight_layout()
    if save:
        plt.savefig("output/graphiques/q3_tips.png", dpi=100)
    plt.close()
    print("    → q3_tips.png")


def fig4_pieges(q4: pd.DataFrame, save: bool = True):
    """Q4 — Barres horizontales de la volumétrie des pièges."""
    fig, ax = plt.subplots(figsize=(10, 5))
    df = q4.sort_values("pct_total", ascending=True)
    colors = ["#FFA726" if p < 5 else "#EF5350" for p in df["pct_total"]]
    ax.barh(df["piege"], df["pct_total"], color=colors)
    ax.set_xlabel("% du total")
    ax.set_title("Q4 — Volumétrie des pièges dans les données brutes")
    plt.tight_layout()
    if save:
        plt.savefig("output/graphiques/q4_pieges.png", dpi=100)
    plt.close()
    print("    → q4_pieges.png")


def fig5_gains(gains: pd.DataFrame, save: bool = True):
    """Gains G1-G4 (G5 est un cas à part)."""
    labels_map = {
        "G1_taille_disque":    "G1\nTaille",
        "G2_lecture_complete": "G2\nLecture\ncomplète",
        "G3_lecture_2cols":    "G3\n2 colonnes\n(star)",
        "G4_requete_1mois":    "G4\nPartitionné",
    }
    df = gains[gains["metrique"].isin(labels_map.keys())].copy()
    df["label"] = df["metrique"].map(labels_map)

    fig, ax = plt.subplots(figsize=(9, 5))
    colors = ["#4CAF50", "#FFC107", "#F44336", "#2196F3"]
    bars = ax.bar(df["label"], df["gain"], color=colors)
    ax.set_ylabel("Gain (×)")
    ax.set_title("Gains mesurés : Parquet + Spark vs CSV + flat")
    for bar, gain in zip(bars, df["gain"]):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 1,
                f"×{gain:.1f}", ha="center", fontweight="bold")
    plt.tight_layout()
    if save:
        plt.savefig("output/graphiques/gains_comparaison.png", dpi=100)
    plt.close()
    print("    → gains_comparaison.png")


def fig6_kmeans(labels: pd.DataFrame, save: bool = True):
    """Q5 — Scatter des zones colorées par cluster (log x, linéaire y)."""
    fig, ax = plt.subplots(figsize=(9, 5))
    for c in sorted(labels["cluster"].unique()):
        sub = labels[labels["cluster"] == c]
        ax.scatter(sub["nb_courses"], sub["revenu_moyen"],
                   label=f"Cluster {c} ({len(sub)} zones)",
                   s=60, alpha=0.7, edgecolor="k", linewidth=0.5)
    ax.set_xlabel("Nombre de courses (log)")
    ax.set_ylabel("Revenu moyen ($)")
    ax.set_xscale("log")
    ax.set_title("Q5 — Segmentation des zones par k-means (k=5)")
    ax.legend(loc="best", fontsize=9)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    if save:
        plt.savefig("output/graphiques/kmeans_clusters.png", dpi=100)
    plt.close()
    print("    → kmeans_clusters.png")