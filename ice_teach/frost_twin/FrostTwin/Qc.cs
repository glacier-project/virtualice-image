// Quality control camera of the local stack (drive mode only; with -lab the line's own cell 5 does it).
// When a twin command with "snap" ends (a pallet stopped on the QC platform, bay 4.2) a camera looks
// straight down on the pallet and the picture goes to Kafka topic ice_data_qc_image (not a line topic):
//
//   {"p": 7, "t": 412.3, "jpg": "<base64>", "content": [0, 11, 12], "color": "yellow"}
//
// content/color: what the twin drew on the pallet, the ground truth to check a vision algorithm against
// (ice_teach/qc_vision.py). 640x480, positions 1-3 left to right, about 0.53 mm per pixel.
using System;
using System.Linq;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using UnityEngine;

namespace FrostTwin
{
    static class Qc
    {
        public const string Topic = "ice_data_qc_image";
        const int W = 640, H = 480;
        const float Height = 0.35f, Fov = 40f;  // camera above the pallet top [m], vertical field of view [deg]
        static IProducer<Null, string> producer;
        static Camera cam;
        static Texture2D shot;      // last picture, also shown on the cell's screen
        static Material screen;
        static bool screenSearched;

        public static void Init(ProducerConfig cfg)
        {
            producer = new ProducerBuilder<Null, string>(cfg).Build();
            Application.quitting += () => { producer.Flush(TimeSpan.FromSeconds(2)); producer.Dispose(); };
        }

        public static void Shoot(Transform pallet, int palletId, JObject content)
        {
            if (producer == null) return;
            try
            {
                if (cam == null)
                {
                    cam = new GameObject("FrostTwin QC camera").AddComponent<Camera>();
                    cam.enabled = false;  // renders only on Shoot
                    cam.fieldOfView = Fov;
                    cam.nearClipPlane = 0.01f;
                    cam.farClipPlane = 2f;
                    cam.targetTexture = new RenderTexture(W, H, 24);
                }
                Bounds b = Parts.PalletBounds(pallet);
                cam.transform.position = new Vector3(b.center.x, b.max.y + Height, b.center.z);
                // image right = positions 1 -> 3 (Parts.Axis): camera right = up x forward
                cam.transform.rotation = Quaternion.LookRotation(Vector3.down, Vector3.Cross(Vector3.down, Parts.Axis(b)));
                cam.Render();

                RenderTexture prev = RenderTexture.active;
                RenderTexture.active = cam.targetTexture;
                if (shot == null) shot = new Texture2D(W, H, TextureFormat.RGB24, false);
                shot.ReadPixels(new Rect(0, 0, W, H), 0, 0);
                shot.Apply();
                RenderTexture.active = prev;
                byte[] jpg = shot.EncodeToJPG(90);
                ShowOnScreen(pallet);

                var msg = new JObject { ["p"] = palletId, ["t"] = Math.Round(Time.time, 3), ["jpg"] = Convert.ToBase64String(jpg) };
                if (content != null) { msg["content"] = content["content"]; msg["color"] = content["color"]; }
                producer.Produce(Topic, new Message<Null, string> { Value = msg.ToString(Newtonsoft.Json.Formatting.None) },
                    r => { if (r.Error.IsError) Debug.LogWarning("FROST TWIN: QC image not sent: " + r.Error.Reason); });
                Debug.Log("FROST TWIN: QC image of pallet " + palletId + " (" + jpg.Length / 1024 + " KB) on " + Topic);
            }
            catch (Exception e) { Debug.LogError("FROST TWIN: QC camera failed: " + e); }
        }

        // The last picture on the display of the quality control cell. The scene's display face
        // (Quality Control/quality/Screen.001, material "Screen") has no usable UVs, so the whole face showed
        // one texel; its mesh is static-batched and not readable at run time. A quad with the picture lies
        // 3 mm in front of it, emissive like a lit screen. Face geometry read from this build's static batch
        // mesh (UnityPy, submesh 7: 4 vertices, world space): centre, normal, width x height.
        static readonly Vector3 FaceCentre = new Vector3(6.8919f, 1.5737f, 2.9163f), FaceNormal = new Vector3(0.898f, 0f, -0.439f);
        const float FaceW = 0.474f, FaceH = 0.308f;

        /// Once, at the first pallet update: the display is black until the first picture.
        public static void Prepare()
        {
            if (screenSearched) return;
            screenSearched = true;
            if (GameObject.Find("Quality Control") == null) { Debug.Log("FROST TWIN: QC screen not found"); return; }
            Vector3 normal = FaceNormal.normalized;
            float k = Mathf.Min(FaceW / W, FaceH / H);  // 4:3 picture inside the face

            GameObject q = GameObject.CreatePrimitive(PrimitiveType.Quad);  // visible side faces -z
            q.name = "FrostTwin QC picture";
            UnityEngine.Object.Destroy(q.GetComponent<Collider>());
            q.transform.SetPositionAndRotation(FaceCentre + normal * 0.003f, Quaternion.LookRotation(-normal, Vector3.up));
            q.transform.localScale = new Vector3(W * k, H * k, 1f);
            var r = q.GetComponent<Renderer>();
            screen = new Material(r.sharedMaterial.shader.name.Contains("Universal") ? r.sharedMaterial : FindLit());
            screen.SetTexture("_BaseMap", null);
            screen.SetColor("_BaseColor", Color.black);
            screen.EnableKeyword("_EMISSION");
            screen.SetColor("_EmissionColor", Color.black);
            r.sharedMaterial = screen;
            Debug.Log("FROST TWIN: QC screen ready");
            CheckScreen(FaceCentre, normal, FaceW);
        }

        // a URP Lit material of the scene (the primitive's default material may not be URP in a build)
        static Material FindLit()
        {
            return Resources.FindObjectsOfTypeAll<Material>().FirstOrDefault(m => m.name == "Screen") ?? Resources.FindObjectsOfTypeAll<Material>().First(m => m.HasProperty("_BaseColor"));
        }

        static void ShowOnScreen(Transform pallet)
        {
            if (screen == null) return;
            screen.SetTexture("_BaseMap", shot);
            screen.SetTexture("_EmissionMap", shot);
            screen.SetColor("_BaseColor", Color.white);
            screen.SetColor("_EmissionColor", Color.white);
        }

        // TEMPORARY: one picture of the screen from in front of it, to check the overlay
        static void CheckScreen(Vector3 centre, Vector3 normal, float size)
        {
            var cam2 = new GameObject("FrostTwin screen check").AddComponent<Camera>();
            cam2.enabled = false;
            cam2.nearClipPlane = 0.01f;
            cam2.targetTexture = new RenderTexture(W, H, 24);
            cam2.transform.position = centre + normal * size * 2f + Vector3.up * 0.1f;
            cam2.transform.LookAt(centre, Vector3.up);
            cam2.fieldOfView = 50f;
            Application.onBeforeRender += Once;
            void Once()
            {
                Application.onBeforeRender -= Once;
                cam2.Render();
                RenderTexture prev = RenderTexture.active;
                RenderTexture.active = cam2.targetTexture;
                var t = new Texture2D(W, H, TextureFormat.RGB24, false);
                t.ReadPixels(new Rect(0, 0, W, H), 0, 0);
                RenderTexture.active = prev;
                System.IO.File.WriteAllBytes(System.IO.Path.Combine(Application.dataPath, "FrostTwin.screen.jpg"), t.EncodeToJPG(90));
                Debug.Log("FROST TWIN: screen check written");
            }
        }
    }
}
