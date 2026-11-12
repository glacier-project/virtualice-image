// Pallet content drawn on the pallets (drive mode), from the twin commands
//   {"p": 7, "content": [c1, c2, c3], "color": "yellow"}
// that KafkaBridge sends when the model's Pallets/PalletN/contentPosition1-3 change. Codes as the line's
// conveyor: 0 empty, 1-3 Duplo 2x2 / 2x4 / 2x8, 10 board, 11 top cover, 12 bottom cover, 13 gadget,
// 14 engraved gadget. Positions 1-3 lie along the pallet's long side, 2 in the middle. Covers and gadgets
// take "color" (the model has none); unknown codes are drawn as a small grey box.
// The stock build draws no content (DrivePallet.FillPalletContentInfo is empty). Plain coloured boxes
// (Duplo 2x2 blue, 2x4 green, 2x8 orange): sharp edges and flat colours for the QC camera (Qc.cs).
using System.Linq;
using Newtonsoft.Json.Linq;
using UnityEngine;

namespace FrostTwin
{
    static class Parts
    {
        const string Root = "FrostTwin parts";

        public static void Show(Transform pallet, JObject cmd)
        {
            Transform old = pallet.Find(Root);
            Renderer[] own = pallet.GetComponentsInChildren<Renderer>().Where(r => old == null || !r.transform.IsChildOf(old)).ToArray();
            if (old != null) Object.Destroy(old.gameObject);
            if (own.Length == 0) return;
            Bounds b = own[0].bounds;
            foreach (Renderer r in own) b.Encapsulate(r.bounds);
            var root = new GameObject(Root).transform;
            root.SetParent(pallet, false);

            Color color = Named((string)cmd["color"]);
            Vector3 axis = Axis(b);
            float step = Vector3.Dot(b.size, axis) * 0.28f;  // positions 1 and 3 still fully on the pallet
            var codes = (JArray)cmd["content"];
            for (int k = 0; k < codes.Count && k < 3; k++)
            {
                int code = (int)codes[k];
                if (code == 0) continue;
                Vector3 at = new Vector3(b.center.x, b.max.y, b.center.z) + axis * (k - 1) * step;  // bottom centre
                switch (code)
                {
                    case 1: case 2: case 3:
                        Brick(root, at, axis, code, own[0].sharedMaterial); break;
                    case 10: Box(root, at, axis, new Vector3(0.06f, 0.004f, 0.04f), new Color(0.0f, 0.35f, 0.1f), own[0].sharedMaterial); break;
                    case 11: Box(root, at, axis, new Vector3(0.07f, 0.012f, 0.05f), color, own[0].sharedMaterial); break;
                    case 12: Box(root, at, axis, new Vector3(0.07f, 0.02f, 0.05f), color, own[0].sharedMaterial); break;
                    case 13: case 14:
                        Box(root, at, axis, new Vector3(0.07f, 0.032f, 0.05f), color, own[0].sharedMaterial);
                        if (code == 14) Box(root, at + Vector3.up * 0.032f, axis, new Vector3(0.03f, 0.001f, 0.01f), Color.white, own[0].sharedMaterial);
                        break;
                    default: Box(root, at, axis, new Vector3(0.02f, 0.02f, 0.02f), Color.grey, own[0].sharedMaterial); break;
                }
            }
        }

        /// Direction of positions 1 -> 3: the long side of the pallet's own bounds (no content).
        public static Vector3 Axis(Bounds pallet) { return pallet.size.x >= pallet.size.z ? Vector3.right : Vector3.forward; }

        public static Bounds PalletBounds(Transform pallet)
        {
            Transform parts = pallet.Find(Root);
            Renderer[] own = pallet.GetComponentsInChildren<Renderer>().Where(r => parts == null || !r.transform.IsChildOf(parts)).ToArray();
            Bounds b = own[0].bounds;
            foreach (Renderer r in own) b.Encapsulate(r.bounds);
            return b;
        }

        static Color Named(string name)
        {
            Color c;
            return !string.IsNullOrEmpty(name) && ColorUtility.TryParseHtmlString(name, out c) ? c : Color.grey;
        }

        // size: x along the pallet's long side, y up, z across
        static void Box(Transform root, Vector3 bottom, Vector3 axis, Vector3 size, Color color, Material material)
        {
            GameObject g = GameObject.CreatePrimitive(PrimitiveType.Cube);
            Object.Destroy(g.GetComponent<Collider>());  // pallets block each other on trigger contacts
            g.transform.localScale = axis == Vector3.right ? size : new Vector3(size.z, size.y, size.x);
            g.transform.position = bottom + Vector3.up * size.y / 2f;
            Paint(g, color, material);
            g.transform.SetParent(root, true);
        }

        static readonly Color[] DuploColors = { Color.blue, new Color(0.05f, 0.55f, 0.15f), new Color(1f, 0.5f, 0f) };  // 2x2, 2x4, 2x8

        // Duplo stud pitch 3.2 cm, brick height 1.9 cm
        static void Brick(Transform root, Vector3 bottom, Vector3 axis, int code, Material material)
        {
            Box(root, bottom, axis, new Vector3(0.032f * (1 << (code - 1)), 0.019f, 0.032f), DuploColors[code - 1], material);
        }

        static void Paint(GameObject g, Color c, Material material)
        {
            foreach (Renderer r in g.GetComponentsInChildren<Renderer>())
            {
                if (material != null) r.sharedMaterial = material;
                Material m = r.material;  // own instance
                if (m.HasProperty("_BaseMap")) m.SetTexture("_BaseMap", null);  // plain colour, no pallet texture
                m.color = c;  // URP Lit: [MainColor] _BaseColor
                if (m.HasProperty("_BaseColor")) m.SetColor("_BaseColor", c);
            }
        }
    }
}
