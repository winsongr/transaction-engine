import concurrent.futures
import random
import sys
import time
import requests

BASE_URL = "http://localhost:8000/api/v1"


def create_transaction():
    response = requests.post(
        f"{BASE_URL}/transactions",
        json={"type": "benchmark", "payload": {"amount": 100}},
        headers={"X-Idempotency-Key": f"bench-{time.time_ns()}"},
    )
    if response.status_code not in (200, 201):
        print(f"Failed to create transaction: {response.text}")
        sys.exit(1)
    return response.json()["id"]


def start_transaction(txn_id):
    response = requests.post(f"{BASE_URL}/transactions/{txn_id}/start")
    if response.status_code != 200:
        print(f"Failed to start transaction: {response.text}")
        sys.exit(1)
    return response.json()


def attempt_transition(txn_id, mode):
    url = f"{BASE_URL}/transactions/{txn_id}/{mode}"
    body = {}
    if mode == "complete":
        body = {"result": {"success": True}}
    elif mode == "fail":
        body = {"error_code": "BENCH_ERR", "error_message": "Benchmark error"}
    elif mode == "cancel":
        body = {"reason": "Benchmark cancel"}

    try:
        response = requests.post(url, json=body)
        return response.status_code, response.text
    except Exception as e:
        return 0, str(e)


def run_benchmark():
    print("🛡  Starting State Safety Benchmark...")

    # 1. Setup
    txn_id = create_transaction()
    print(f"1. Created Transaction: {txn_id}")

    start_transaction(txn_id)
    print("2. Started Transaction (State: PENDING)")

    # 2. Attack
    CONCURRENCY = 50
    print(f"3. Launching {CONCURRENCY} concurrent transition attempts...")

    modes = ["complete", "fail", "cancel"]

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = []
        for _ in range(CONCURRENCY):
            mode = random.choice(modes)
            futures.append(executor.submit(attempt_transition, txn_id, mode))

        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    # 3. Analyze
    successes = [r for r in results if r[0] == 200]
    failures = [r for r in results if r[0] in (400, 409, 422)]
    server_errors = [r for r in results if r[0] >= 500 or r[0] == 0]

    print("\nResults:")
    print(f"✅ Successful Transitions: {len(successes)}")
    print(f"🚫 Rejected Transitions:   {len(failures)}")
    print(f"⚠️  Server Errors:         {len(server_errors)}")

    # 4. Assertions
    if len(server_errors) > 0:
        print("\n❌ FAILED: Server errors detected.")
        sys.exit(1)

    if len(successes) != 1:
        print(f"\n❌ FAILED: Expected exactly 1 success, got {len(successes)}")
        sys.exit(1)

    if len(failures) != CONCURRENCY - 1:
        print(
            f"\n❌ FAILED: Expected {CONCURRENCY - 1} rejections, got {len(failures)}"
        )
        sys.exit(1)

    # 5. Final State Check
    final_resp = requests.get(f"{BASE_URL}/transactions/{txn_id}")
    final_state = final_resp.json()["status"]
    final_version = final_resp.json()["version"]

    print(f"\nFinal State:   {final_state}")
    print(f"Final Version: {final_version}")

    expected_version = 3  # 1(Create) -> 2(Start) -> 3(Terminal)

    if final_version != expected_version:
        print(
            f"❌ FAILED: Implementation error. Version gap detected (Expected {expected_version}, got {final_version})"
        )
        sys.exit(1)

    print("\n🎉 VALIDATION PASSED: Invariants held under fire.")


if __name__ == "__main__":
    try:
        run_benchmark()
    except KeyboardInterrupt:
        pass
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to API. Is it running on localhost:8000?")
