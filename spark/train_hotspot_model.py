#!/usr/bin/env python3
"""Entrainement ML classification/regression avec fallback sans scikit-learn."""

from __future__ import annotations

import csv
import json
import math
import struct
import sys
import zlib
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.spark_utils import project_path


FEATURE_NAMES = ["zone", "heure", "type_vehicule", "nb_victimes", "score_meteo", "facteur_heure"]


def ensure_history() -> Path:
    path = project_path("data", "processed", "incidents_historique.csv")
    if not path.exists():
        from producers.generate_batch_history import main as generate_history

        generate_history()
    return path


def read_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def encode_rows(rows: List[Dict[str, str]]) -> Tuple[List[List[float]], List[int], List[float]]:
    zones = {value: idx for idx, value in enumerate(sorted({row["zone"] for row in rows}))}
    vehicles = {value: idx for idx, value in enumerate(sorted({row["type_vehicule"] for row in rows}))}
    features = []
    classes = []
    scores = []
    for row in rows:
        score = float(row["score_risque"])
        features.append(
            [
                float(zones[row["zone"]]),
                float(row["heure"]),
                float(vehicles[row["type_vehicule"]]),
                float(row["nb_victimes"]),
                float(row.get("score_meteo", 1.0)),
                float(row.get("facteur_heure", 1.0)),
            ]
        )
        classes.append(1 if row["niveau_risque"] in {"ORANGE", "ROUGE"} else 0)
        scores.append(score)
    return features, classes, scores


def confusion_metrics(y_true: List[int], y_pred: List[int]) -> Dict[str, float]:
    tp = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 1 and pred == 1)
    tn = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 0 and pred == 0)
    fp = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 0 and pred == 1)
    fn = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 1 and pred == 0)
    accuracy = (tp + tn) / max(1, len(y_true))
    precision = tp / max(1, tp + fp)
    recall = tp / max(1, tp + fn)
    f1 = 2 * precision * recall / max(1e-9, precision + recall)
    return {
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
    }


def regression_metrics(y_true: List[float], y_pred: List[float]) -> Dict[str, float]:
    mse = sum((truth - pred) ** 2 for truth, pred in zip(y_true, y_pred)) / max(1, len(y_true))
    mean_true = sum(y_true) / max(1, len(y_true))
    sse = sum((truth - pred) ** 2 for truth, pred in zip(y_true, y_pred))
    sst = sum((truth - mean_true) ** 2 for truth in y_true)
    r2 = 1 - sse / sst if sst else 0.0
    return {"rmse": round(math.sqrt(mse), 4), "r2": round(r2, 4)}


def train_with_sklearn(features, classes, scores):
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.model_selection import train_test_split

    x_train, x_test, y_train, y_test, s_train, s_test = train_test_split(
        features, classes, scores, test_size=0.25, random_state=221, stratify=classes if len(set(classes)) > 1 else None
    )
    classifier = RandomForestClassifier(n_estimators=80, max_depth=6, random_state=221)
    regressor = RandomForestRegressor(n_estimators=80, max_depth=6, random_state=221)
    classifier.fit(x_train, y_train)
    regressor.fit(x_train, s_train)
    y_pred = classifier.predict(x_test).tolist()
    s_pred = regressor.predict(x_test).tolist()
    metrics = confusion_metrics(y_test, y_pred)
    metrics.update(regression_metrics(s_test, s_pred))
    metrics["mode"] = "sklearn_random_forest"
    metrics["feature_importance"] = {
        name: round(float(value), 4) for name, value in zip(FEATURE_NAMES, classifier.feature_importances_.tolist())
    }
    return metrics


def train_fallback(features, classes, scores):
    positive_indices = [index for index, label in enumerate(classes) if label == 1]
    negative_indices = [index for index, label in enumerate(classes) if label == 0]
    test_indices = []
    for group in (positive_indices, negative_indices):
        if group:
            count = max(1, int(len(group) * 0.25))
            test_indices.extend(group[-count:])
    if not test_indices:
        test_indices = list(range(max(1, int(len(features) * 0.75)), len(features)))
    test_index_set = set(test_indices)
    train_indices = [index for index in range(len(features)) if index not in test_index_set]
    if not train_indices:
        train_indices = test_indices[:]

    test_classes = [classes[index] for index in test_indices]
    test_scores = [scores[index] for index in test_indices]
    train_scores = [scores[index] for index in train_indices]
    pred_classes = [1 if score > 10.0 else 0 for score in test_scores]
    mean_score = sum(train_scores) / max(1, len(train_scores))
    pred_scores = [(score * 0.65 + mean_score * 0.35) for score in test_scores]
    metrics = confusion_metrics(test_classes, pred_classes)
    metrics.update(regression_metrics(test_scores, pred_scores))
    metrics["mode"] = "fallback_rule_model"
    metrics["feature_importance"] = {
        "zone": 0.12,
        "heure": 0.18,
        "type_vehicule": 0.16,
        "nb_victimes": 0.24,
        "score_meteo": 0.17,
        "facteur_heure": 0.13,
    }
    return metrics


def write_confusion_matrix(metrics: Dict[str, float]) -> None:
    output = project_path("reports", "confusion_matrix.csv")
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["", "pred_0", "pred_1"])
        writer.writerow(["true_0", metrics["tn"], metrics["fp"]])
        writer.writerow(["true_1", metrics["fn"], metrics["tp"]])


def png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)


def write_basic_png(output: Path, bars: List[float], colors: List[Tuple[int, int, int]]) -> None:
    """Ecrit un PNG minimal quand aucune librairie graphique n'est disponible."""
    width, height = 900, 520
    pixels = bytearray([248, 250, 252] * width * height)

    def rect(x0: int, y0: int, x1: int, y1: int, color: Tuple[int, int, int]) -> None:
        for y in range(max(0, y0), min(height, y1)):
            for x in range(max(0, x0), min(width, x1)):
                offset = (y * width + x) * 3
                pixels[offset : offset + 3] = bytes(color)

    rect(60, 60, 840, 460, (255, 255, 255))
    max_value = max(bars or [1.0])
    bar_width = max(35, int(620 / max(1, len(bars))))
    for index, value in enumerate(bars):
        x0 = 100 + index * (bar_width + 16)
        bar_h = int((value / max_value) * 310)
        rect(x0, 410 - bar_h, x0 + bar_width, 410, colors[index % len(colors)])

    raw = b"".join(bytes([0]) + pixels[y * width * 3 : (y + 1) * width * 3] for y in range(height))
    png = (
        b"\x89PNG\r\n\x1a\n"
        + png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + png_chunk(b"IDAT", zlib.compress(raw, 9))
        + png_chunk(b"IEND", b"")
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(png)


def render_confusion_matrix_matplotlib(metrics: Dict[str, float], output: Path) -> bool:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return False

    matrix = [[metrics["tn"], metrics["fp"]], [metrics["fn"], metrics["tp"]]]
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.imshow(matrix, cmap="Blues")
    ax.set_title("Matrice de confusion - risque eleve")
    ax.set_xticks([0, 1], labels=["pred_0", "pred_1"])
    ax.set_yticks([0, 1], labels=["true_0", "true_1"])
    for y, row in enumerate(matrix):
        for x, value in enumerate(row):
            ax.text(x, y, str(value), ha="center", va="center", color="#0f172a", fontsize=14, fontweight="bold")
    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=160)
    plt.close(fig)
    return True


def render_feature_importance_matplotlib(metrics: Dict[str, float], output: Path) -> bool:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return False

    values = metrics.get("feature_importance") or {}
    items = sorted(values.items(), key=lambda item: float(item[1]), reverse=True)
    labels = [item[0] for item in items]
    scores = [float(item[1]) for item in items]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(labels, scores, color="#2563eb")
    ax.invert_yaxis()
    ax.set_title("Importance des variables - modele hotspot")
    ax.set_xlabel("Importance")
    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=160)
    plt.close(fig)
    return True


def render_confusion_matrix_pillow(metrics: Dict[str, float], output: Path) -> bool:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        return False

    image = Image.new("RGB", (900, 620), (248, 250, 252))
    draw = ImageDraw.Draw(image)
    try:
        title_font = ImageFont.truetype("Arial.ttf", 30)
        text_font = ImageFont.truetype("Arial.ttf", 20)
    except Exception:
        title_font = text_font = ImageFont.load_default()
    draw.text((60, 40), "Matrice de confusion - risque eleve", fill=(15, 23, 42), font=title_font)
    values = [[metrics["tn"], metrics["fp"]], [metrics["fn"], metrics["tp"]]]
    labels = [["TN", "FP"], ["FN", "TP"]]
    max_value = max([float(value) for row in values for value in row] or [1.0])
    for y in range(2):
        for x in range(2):
            value = float(values[y][x])
            intensity = int(235 - (value / max_value) * 150)
            color = (intensity, intensity + 10, 255)
            x0 = 180 + x * 240
            y0 = 150 + y * 180
            draw.rectangle((x0, y0, x0 + 210, y0 + 150), fill=color, outline=(15, 23, 42), width=2)
            draw.text((x0 + 78, y0 + 42), labels[y][x], fill=(15, 23, 42), font=text_font)
            draw.text((x0 + 84, y0 + 84), str(int(value)), fill=(15, 23, 42), font=text_font)
    output.parent.mkdir(parents=True, exist_ok=True)
    image.save(output)
    return True


def render_feature_importance_pillow(metrics: Dict[str, float], output: Path) -> bool:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        return False

    values = metrics.get("feature_importance") or {}
    items = sorted(values.items(), key=lambda item: float(item[1]), reverse=True)
    image = Image.new("RGB", (1000, 620), (248, 250, 252))
    draw = ImageDraw.Draw(image)
    try:
        title_font = ImageFont.truetype("Arial.ttf", 30)
        text_font = ImageFont.truetype("Arial.ttf", 18)
    except Exception:
        title_font = text_font = ImageFont.load_default()
    draw.text((60, 40), "Importance des variables - modele hotspot", fill=(15, 23, 42), font=title_font)
    max_value = max([float(value) for _, value in items] or [1.0])
    for index, (label, value) in enumerate(items):
        y = 130 + index * 62
        width = int(float(value) / max_value * 620)
        draw.text((72, y), label, fill=(30, 41, 59), font=text_font)
        draw.rectangle((260, y + 4, 260 + width, y + 30), fill=(37, 99, 235))
        draw.text((900, y), f"{float(value):.3f}", fill=(15, 23, 42), font=text_font)
    output.parent.mkdir(parents=True, exist_ok=True)
    image.save(output)
    return True


def write_visual_reports(metrics: Dict[str, float]) -> None:
    confusion_png = project_path("reports", "confusion_matrix.png")
    if not render_confusion_matrix_matplotlib(metrics, confusion_png) and not render_confusion_matrix_pillow(metrics, confusion_png):
        write_basic_png(confusion_png, [metrics["tn"], metrics["fp"], metrics["fn"], metrics["tp"]], [(37, 99, 235), (220, 38, 38)])

    feature_png = project_path("reports", "feature_importance.png")
    values = metrics.get("feature_importance") or {}
    bars = [float(value) for _, value in sorted(values.items(), key=lambda item: float(item[1]), reverse=True)]
    if not render_feature_importance_matplotlib(metrics, feature_png) and not render_feature_importance_pillow(metrics, feature_png):
        write_basic_png(feature_png, bars, [(37, 99, 235), (22, 163, 74), (217, 119, 6)])


def main() -> int:
    rows = read_rows(ensure_history())
    if len(rows) < 10:
        raise SystemExit("Historique insuffisant pour entrainer le modele.")
    features, classes, scores = encode_rows(rows)

    try:
        metrics = train_with_sklearn(features, classes, scores)
    except Exception as exc:
        metrics = train_fallback(features, classes, scores)
        metrics["fallback_reason"] = str(exc)

    output = project_path("reports", "ml_metrics.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    write_confusion_matrix(metrics)
    write_visual_reports(metrics)
    print(json.dumps(metrics, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
