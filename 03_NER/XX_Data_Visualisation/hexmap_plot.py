
import json
import matplotlib.pyplot as plt
from matplotlib.patches import RegularPolygon
import numpy as np
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import matplotlib.image as mpimg

def plot_hex_map(daten: dict, titel: str = "", untertitel: str = ""):
    with open("data/hexmap.json", "r") as f:
        mapdata = json.load(f)

    width = mapdata["width"]
    height = mapdata["height"]
    tile_data = mapdata["layers"][0]["data"]

    feldbeschriftung = {
        1: "GE", 3: "NE", 9: "VD", 10: "SO", 11: "JU", 12: "BS",
        15: "VS", 16: "FR", 17: "BE", 18: "BL", 23: "OW", 24: "LU",
        25: "AG", 26: "ZH", 29: "TI", 30: "NW", 31: "SZ", 32: "ZG",
        33: "TG", 34: "SH", 37: "UR", 38: "GL", 39: "AR", 40: "SG",
        44: "GR", 46: "AI"
    }

    werte = [v for v in daten.values() if v > 0]
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

    margin_left = 2.5
    margin_right = 3.5
    margin_top = 4
    margin_bottom = 0.5

    background = mpimg.imread("data/backgroundmap.jpg")
    img_height, img_width = background.shape[0], background.shape[1]
    img_ratio = img_width / img_height

    plot_x_min = x_min + margin_left
    plot_x_max = x_max - margin_right
    plot_y_min = y_min + margin_bottom
    plot_y_max = y_max - margin_top
    plot_width = plot_x_max - plot_x_min
    plot_height = plot_y_max - plot_y_min
    plot_ratio = plot_width / plot_height

    if img_ratio > plot_ratio:
        new_width = plot_height * img_ratio
        x_center = (plot_x_min + plot_x_max) / 2
        extent = [x_center - new_width / 2, x_center + new_width / 2, plot_y_min, plot_y_max]
    else:
        new_height = plot_width / img_ratio
        y_center = (plot_y_min + plot_y_max) / 2
        extent = [plot_x_min, plot_x_max, y_center - new_height / 2, y_center + new_height / 2]

    ax.imshow(background, extent=extent, aspect='equal', zorder=0, alpha=0.3)

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
            ha='center', va='bottom', fontsize=16, weight='bold', zorder=3)
    ax.text((x_min + x_max) / 2, y_max - 3, untertitel,
            ha='center', va='bottom', fontsize=14, zorder=3)

    sm = cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, fraction=0.03, pad=0.04)
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
        y_ch = y_max - hex_radius * 2.5

        hex_ch = RegularPolygon((x_ch, y_ch), numVertices=6, radius=hex_radius,
                                orientation=np.radians(0),
                                facecolor=ch_farbe, edgecolor=edge, zorder=2)
        ax.add_patch(hex_ch)

        ax.text(x_ch, y_ch + 0.5, "CH", ha='center', va='center',
                fontsize=14, weight=gewicht, color=textfarbe, zorder=3)
        if ch_wert > 0:
            ax.text(x_ch, y_ch - 0.7, str(ch_wert), ha='center', va='center',
                    fontsize=12, color=textfarbe, zorder=3)

        ax.text(x_ch + 2, y_ch, "überkantonale oder\ngesamtschweizerische Datensätze", 
                ha='left', va='center',
                fontsize=12, color=textfarbe, zorder=3)

    plt.axis('off')
    plt.tight_layout()
    plt.show()
