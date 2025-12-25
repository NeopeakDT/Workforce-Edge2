import cv2
from pathlib import Path

QA_DIR = Path("1.pseudo_labeled")
IMG_DIR = QA_DIR / "images"
LBL_DIR = QA_DIR / "labels"

CLASS_NAMES = {
    0: "cow",
    1: "person",
    2: "scrapping_tool",
    3: "tmr_machine",
    4: "tractor",
}

COLORS = {
    0: (0, 255, 0),
    1: (255, 0, 0),
    2: (0, 255, 255),
    3: (255, 255, 0),
    4: (0, 0, 255),
}

out_dir = Path("3.qa_preview")
out_dir.mkdir(exist_ok=True)

for img_path in IMG_DIR.rglob("*.jpg"):
    rel = img_path.relative_to(IMG_DIR)
    lbl_path = LBL_DIR / rel.with_suffix(".txt")

    img = cv2.imread(str(img_path))
    h, w = img.shape[:2]

    if lbl_path.exists():
        with open(lbl_path) as f:
            for line in f:
                cls, xc, yc, bw, bh = map(float, line.split())
                cls = int(cls)

                x1 = int((xc - bw / 2) * w)
                y1 = int((yc - bh / 2) * h)
                x2 = int((xc + bw / 2) * w)
                y2 = int((yc + bh / 2) * h)

                cv2.rectangle(img, (x1, y1), (x2, y2), COLORS[cls], 2)
                cv2.putText(
                    img,
                    CLASS_NAMES[cls],
                    (x1, y1 - 6),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.6,
                    COLORS[cls],
                    2,
                )

    cv2.imwrite(str(out_dir / rel.name), img)
    print(f"✅ Saved: {rel.name}")

print(f"\n📁 All images saved to: {out_dir}")
