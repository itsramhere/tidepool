"""Run real loopback load scenarios; preserve JSON, server logs and local data."""
import argparse
import datetime
import json
import pathlib
import subprocess
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", default="10s")
    args = parser.parse_args()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    work = ROOT / "data" / ("benchmark-" + stamp)
    work.mkdir(parents=True)
    assert work.resolve().is_relative_to((ROOT / "data").resolve())
    ext = ".exe" if __import__("os").name == "nt" else ""
    for name in ("tidepoold", "tpload", "tpctl"):
        subprocess.run(["go", "build", "-o", str(work / (name + ext)), "./cmd/" + name], cwd=ROOT, check=True)
    token = subprocess.check_output([str(work / ("tpctl" + ext)), "token", "--secret", "benchmark-secret-local", "--uid", "benchmark"], cwd=ROOT, text=True).strip()
    for clients, rate in ((2, 200), (16, 500), (64, 1000)):
        config = work / (f"node-{clients}.yaml")
        config.write_text("\n".join([
            "node_id: benchmark", 'listen: "127.0.0.1:7300"',
            'admin_listen: "127.0.0.1:7301"', "admin_token: benchmark-admin-local",
            "data_dir: " + json.dumps(str(work / f"db-{clients}")), "fsync: always",
            "jwt_secret: benchmark-secret-local", "rules_file: bench/rules.yaml",
            "session_send_buffer: 4096", ""
        ]), encoding="utf-8")
        log_path = ROOT / "bench" / f"load-{clients}-server.log"
        with log_path.open("w", encoding="utf-8") as log:
            node = subprocess.Popen([str(work / ("tidepoold" + ext)), "--config", str(config)], cwd=ROOT, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 20
                while True:
                    if node.poll() is not None:
                        raise RuntimeError(f"node failed: {log_path.read_text()}")
                    try:
                        with urllib.request.urlopen("http://127.0.0.1:7300/healthz", timeout=1) as response:
                            if response.status == 200:
                                break
                    except OSError:
                        pass
                    if time.monotonic() > deadline:
                        raise RuntimeError("node health timeout")
                    time.sleep(.1)
                result = subprocess.run([
                    str(work / ("tpload" + ext)), "--url", "ws://127.0.0.1:7300/v1/sync",
                    "--token", token, "--clients", str(clients), "--docs", "100",
                    "--rate", str(rate), "--duration", args.duration
                ], cwd=ROOT, text=True, capture_output=True, timeout=180)
                (ROOT / "bench" / f"load-{clients}.json").write_text(result.stdout, encoding="utf-8")
                if result.stderr:
                    print(result.stderr)
                if result.returncode:
                    raise RuntimeError(f"load failed ({result.returncode}): {result.stdout}")
                with urllib.request.urlopen("http://127.0.0.1:7300/metrics") as response:
                    (ROOT / "bench" / f"load-{clients}-metrics.txt").write_bytes(response.read())
                print(f"clients={clients} rate={rate}: {result.stdout}", flush=True)
            finally:
                node.terminate()
                node.wait(timeout=10)
    print(f"Raw database data retained at {work}")


if __name__ == "__main__":
    main()
