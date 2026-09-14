export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
          "Access-Control-Allow-Headers": "*"
        }
      });
    }

    const file = url.searchParams.get("file");
    const name = url.searchParams.get("name") || "video.mkv";
    const repo = url.searchParams.get("repo") || "ausable14/DB";

    if (!file) {
      return new Response("Archive Downloader Ready. Usage: ?file=<disguised_file>&name=<original_name>&repo=<repo_id>", {
        status: 200,
        headers: { "Content-Type": "text/plain; charset=utf-8" }
      });
    }

    const hfUrl = "https://huggingface.co/datasets/" + repo + "/resolve/main/" + file;

    const fetchHeaders = {};
    if (request.headers.get("range")) {
      fetchHeaders["Range"] = request.headers.get("range");
    }

    const response = await fetch(hfUrl, { headers: fetchHeaders });

    if (!response.ok && response.status !== 206) {
      return new Response("Hugging Face error: " + response.status + " " + response.statusText, {
        status: response.status
      });
    }

    const headers = new Headers(response.headers);
    const safeName = name.replace(/["\r\n]/g, "_");
    headers.set("Content-Disposition", 'attachment; filename="' + safeName + '"; filename*=UTF-8\'\'' + encodeURIComponent(name));
    headers.set("Content-Type", "application/octet-stream");
    headers.set("Access-Control-Allow-Origin", "*");

    return new Response(response.body, {
      status: response.status,
      headers: headers
    });
  }
};
