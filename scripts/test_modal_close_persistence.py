#!/usr/bin/env python3
"""E2E test: incident detail modal must stay closed after close + live sync."""

import json
import sys
import time
import urllib.error
import urllib.request

from playwright.sync_api import sync_playwright, expect

BASE = "http://localhost:8005"
POLL_WAIT_SEC = 4.5  # live sync polls every 3s


def api_get(path):
    with urllib.request.urlopen(f"{BASE}{path}") as resp:
        return json.loads(resp.read().decode())


def api_post(path, payload):
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode()) if resp.length else {}


def bump_last_modified(year, week, data_type="Incident"):
    """Save a no-op update to bump last_modified and trigger silent poll apply."""
    data = api_get(f"/api/data?year={year}&week={week}&type={data_type}")
    items = data.get("items") or []
    if not items:
        raise RuntimeError("No incidents in current week to test with")
    item = dict(items[0])
    api_post(
        "/api/data",
        {
            "year": year,
            "week": week,
            "type": data_type,
            "action": item,
            "is_update": True,
        },
    )
    return item


def wait_for_table_rows(page, min_rows=1, timeout_ms=15000):
    page.wait_for_selector("tbody tr td[data-id]", timeout=timeout_ms)
    rows = page.locator("tbody tr td[data-id]")
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        if rows.count() >= min_rows:
            return
        page.wait_for_timeout(200)
    raise TimeoutError(f"Expected at least {min_rows} table rows with data-id")


def modal_is_open(page):
    modal = page.locator("#incident-detail-modal")
    return modal.evaluate("el => el.style.display === 'block'")


def test_modal_stays_closed_after_sync(page):
    page.goto(BASE, wait_until="networkidle")
    wait_for_table_rows(page)

    first_row = page.locator("tbody tr").first
    first_row.click()
    page.wait_for_selector("#incident-detail-modal", state="visible", timeout=5000)
    assert modal_is_open(page), "Modal should open after row click"

    page.locator("#close-detail-modal").click()
    page.wait_for_timeout(300)
    assert not modal_is_open(page), "Modal should be closed after clicking close"

    # Stale DOM still has h3[data-id] — old bug would reopen on next sync
    has_stale_id = page.evaluate(
        """() => !!document.querySelector('.detail-title-row h3')?.getAttribute('data-id')"""
    )
    assert has_stale_id, "Precondition: stale data-id should still exist in hidden DOM"

    year = page.evaluate("() => currentYear")
    week = page.evaluate("() => currentWeek")
    bump_last_modified(year, week)

    page.wait_for_timeout(int(POLL_WAIT_SEC * 1000))
    assert not modal_is_open(page), (
        "BUG: Modal reopened after close when live sync received updated last_modified"
    )

    open_id = page.evaluate("() => openDetailItemId")
    assert open_id is None, f"openDetailItemId should be null after close, got {open_id!r}"


def test_actions_tab_persists_through_sync(page):
    page.goto(BASE, wait_until="networkidle")
    wait_for_table_rows(page)

    page.locator("tbody tr").first.click()
    page.wait_for_selector("#incident-detail-modal", state="visible")

    actions_tab = page.locator("#tab-items")
    if not actions_tab.is_visible():
        print("SKIP: No actions tab on this view")
        return

    page.locator('label[for="tab-items"]').click()
    expect(actions_tab).to_be_checked()

    year = page.evaluate("() => currentYear")
    week = page.evaluate("() => currentWeek")
    bump_last_modified(year, week)

    page.wait_for_timeout(int(POLL_WAIT_SEC * 1000))
    assert modal_is_open(page), "Modal should remain open during in-place sync refresh"
    expect(actions_tab).to_be_checked()
    assert page.evaluate("() => activeDetailSession?.raState?.currentTab") == "actions"


def main():
    try:
        api_get("/api/data?year=2026&week=24&type=Incident")
    except urllib.error.URLError as e:
        print(f"FAIL: Server not reachable at {BASE}: {e}")
        sys.exit(1)

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.on("pageerror", lambda err: print(f"PAGE ERROR: {err}"))

        tests = [
            ("modal_stays_closed_after_sync", test_modal_stays_closed_after_sync),
            ("actions_tab_persists_through_sync", test_actions_tab_persists_through_sync),
        ]
        for name, fn in tests:
            try:
                fn(page)
                results.append((name, True, None))
                print(f"PASS: {name}")
            except Exception as e:
                results.append((name, False, str(e)))
                print(f"FAIL: {name} — {e}")

        browser.close()

    failed = [r for r in results if not r[1]]
    if failed:
        sys.exit(1)
    print(f"\nAll {len(results)} tests passed.")


if __name__ == "__main__":
    main()
