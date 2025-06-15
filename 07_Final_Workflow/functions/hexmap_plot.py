import json
import matplotlib.pyplot as plt
from matplotlib.patches import RegularPolygon
import numpy as np
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import os

def plot_hex_map(daten: dict, titel: str = "", untertitel: str = ""):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    hexmap_path = os.path.join(script_dir, "../data", "hexmap.json")

    with open(hexmap_path, "r") as f:
        hexdata = json.load(f)

    width = hexdata["width"]
    height = hexdata["height"]
    tile_data = hexdata["layers"][0]["data"]

    feldbeschriftung = {
        1: "GE", 3: "NE", 9: "VD", 10: "SO", 11: "JU", 12: "BS",
        15: "VS", 16: "FR", 17: "BE", 18: "BL", 23: "OW", 24: "LU",
        25: "AG", 26: "ZH", 29: "TI", 30: "NW", 31: "SZ", 32: "ZG",
        33: "TG", 34: "SH", 37: "UR", 38: "GL", 39: "SG", 40: "AR",
        44: "GR", 46: "AI"
    }

    werte = [v for k, v in daten.items() if k != "None" and v > 0]
    vmin = 0
    vmax = max(werte) if werte else 1
    cmap = cm.get_cmap("Greens")
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)

    fig, ax = plt.subplots(figsize=(12, 10))
    ax.set_aspect('equal')

    hex_radius = 2
    spacing_factor = 1.2

    x_min = -hex_radius * 1.5
    x_max = spacing_factor * np.sqrt(3) * hex_radius * width + hex_radius * 1.5
    y_min = -hex_radius * 1.5
    y_max = spacing_factor * 1.5 * hex_radius * height + hex_radius * 1.5

    for i, tile_id in enumerate(tile_data):
        if tile_id == 0:
            continue

        row = i % height
        col = i // height
        x = spacing_factor * np.sqrt(3) * hex_radius * (col + 0.5 * (row % 2))
        y = spacing_factor * 1.5 * hex_radius * row

        kanton = feldbeschriftung.get(i)
        wert = daten.get(kanton, None)

        if wert is not None and wert > 0:
            farbe = cmap(norm(wert))
            edge = 'black'
            textfarbe = 'black'
            gewicht = 'bold'
        else:
            farbe = "white"
            edge = 'gray'
            textfarbe = 'gray'
            gewicht = 'normal'

        hexagon = RegularPolygon((x, y), numVertices=6, radius=hex_radius,
                                 orientation=np.radians(0),
                                 facecolor=farbe, edgecolor=edge, zorder=1)
        ax.add_patch(hexagon)

        if kanton:
            ax.text(x, y + 0.5, kanton, ha='center', va='center',
                    fontsize=14, color=textfarbe, weight=gewicht, zorder=2)
            if wert is not None and wert > 0:
                ax.text(x, y - 0.7, str(wert), ha='center', va='center',
                        fontsize=14, color=textfarbe, zorder=2)

    ax.set_xlim(x_min, x_max)
    ax.set_ylim(y_min, y_max)

    ax.text((x_min + x_max) / 2, y_max - 2, titel,
            ha='center', va='bottom', fontsize=20, weight='bold', zorder=3)
    ax.text((x_min + x_max) / 2, y_max - 3, untertitel,
            ha='center', va='bottom', fontsize=14, zorder=3)

    sm = cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, fraction=0.03, pad=-0.1)
    cbar.set_label("Anzahl Datensätze", fontsize=14)
    cbar.ax.tick_params(labelsize=12)

    ax.set_ylim(y_min, y_max - 1)

    ch_wert = daten.get("CH", None)
    if ch_wert is not None:
        if ch_wert > 0:
            ch_norm = (ch_wert - vmin) / (vmax - vmin)
            ch_farbe = cmap(ch_norm)
            edge = 'black'
            textfarbe = 'black'
            gewicht = 'bold'
        else:
            ch_farbe = "white"
            edge = 'gray'
            textfarbe = 'gray'
            gewicht = 'normal'

        x_ch = x_min + hex_radius * 1.5
        y_ch = y_max - hex_radius * 3.2

        hex_ch = RegularPolygon((x_ch, y_ch), numVertices=6, radius=hex_radius,
                                orientation=np.radians(0),
                                facecolor=ch_farbe, edgecolor=edge, zorder=2)
        ax.add_patch(hex_ch)

        ax.text(x_ch, y_ch + 0.5, "CH", ha='center', va='center',
                fontsize=14, weight=gewicht, color=textfarbe, zorder=3)
        if ch_wert > 0:
            ax.text(x_ch, y_ch - 0.7, str(ch_wert), ha='center', va='center',
                    fontsize=14, color=textfarbe, weight=gewicht, zorder=3)

        ax.text(x_ch + 2, y_ch, "überkantonale oder\ngesamtschweizerische Datensätze", 
                ha='left', va='center',
                fontsize=12, color=textfarbe, zorder=3)

    none_wert = daten.get("None", 0)
    if none_wert > 0:
        ax.text(x_min + 1, y_min + 1, f"Zusätzlich {none_wert} Suchergebnisse ohne erkannte räumliche Ausdehnung gefunden.",
                ha='left', va='bottom', fontsize=14, color='black', zorder=3)

    plt.axis('off')
    plt.tight_layout()
    plt.show()
