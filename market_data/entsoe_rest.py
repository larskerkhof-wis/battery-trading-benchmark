# market_data/entsoe_rest.py
import io
import zipfile
import requests
import pandas as pd
import xml.etree.ElementTree as ET

ENTSOE_BASE = "https://web-api.tp.entsoe.eu/api"

def _to_utc_ts(ts: str) -> pd.Timestamp:
    # ts kan "2025-10-20T00:00Z" of met offset zijn
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    return t

def _parse_a85_xml(xml_bytes: bytes, tz_out: str = "Europe/Amsterdam") -> pd.DataFrame:
    """
    Parseert één Balancing_MarketDocument (A85) naar een DataFrame met 15T index
    en kolommen ['charge_price','discharge_price'].
    Als er maar één prijsreeks is, wordt die gedupliceerd voor beide kolommen.
    """
    root = ET.fromstring(xml_bytes)
    ns = {"ns": root.tag.split('}')[0].strip('{')}

    # Verzamel alle Period-blokken
    series = []
    for per in root.findall(".//ns:Period", ns):
        pstart = per.findtext("./ns:timeInterval/ns:start", namespaces=ns)
        if not pstart:
            continue
        pres = per.findtext("./ns:resolution", namespaces=ns) or "PT15M"
        step = {"PT15M": "15min", "PT60M": "60min"}.get(pres, "15min")
        start_utc = _to_utc_ts(pstart)

        vals = []
        # probeer eerst <imbalance_Price.amount>, anders <price.amount>
        for pt in per.findall("./ns:Point", ns):
            v = pt.findtext("./ns:imbalance_Price.amount", namespaces=ns)
            if v is None:
                v = pt.findtext("./ns:price.amount", namespaces=ns)
            if v is None:
                continue
            vals.append(float(v))

        if not vals:
            continue

        idx = pd.date_range(start_utc, periods=len(vals), freq=step, tz="UTC")
        series.append(pd.Series(vals, index=idx))

    if not series:
        return pd.DataFrame()

    s = pd.concat(series).sort_index()

    # Uniformeer naar 15T raster (DST-proof): links-inclusief, rechts-exclusief
    full_idx = pd.date_range(
        s.index.min(),
        s.index.max() + pd.Timedelta(minutes=15),
        freq="15min",
        tz="UTC",
        inclusive="left",
    )
    s = s.reindex(full_idx)

    df = pd.DataFrame(
        {
            "charge_price": s.values,
            "discharge_price": s.values,  # zelfde kolom als er maar 1 reeks is
        },
        index=full_idx.tz_convert(tz_out),
    )
    df.index.name = "timestamp"
    return df

def get_imbalance_prices_a85(
    token: str,
    control_area_domain: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    tz: str = "Europe/Amsterdam",
    chunk_hours: int = 36,
) -> pd.DataFrame:
    """
    Haalt A85 onbalansprijzen op via REST (gechunked om 400/413 te vermijden)
    en geeft een 15-min DataFrame terug met kolommen charge/discharge.
    """
    if start.tz is None:
        start = start.tz_localize(tz)
    if end.tz is None:
        end = end.tz_localize(tz)

    frames = []
    rng = pd.interval_range(start, end, freq=f"{chunk_hours}H", closed="left")
    if len(rng) == 0:
        rng = [pd.Interval(left=start, right=end, closed="left")]

    for iv in rng:
        ps = iv.left.tz_convert("UTC").strftime("%Y%m%d%H%M")
        pe = iv.right.tz_convert("UTC").strftime("%Y%m%d%H%M")

        params = {
            "securityToken": token,
            "documentType": "A85",
            "controlArea_Domain": control_area_domain,
            "periodStart": ps,
            "periodEnd": pe,
        }
        r = requests.get(ENTSOE_BASE, params=params, timeout=60)
        # 200 OK met ZIP of XML wordt verwacht
        r.raise_for_status()

        content = r.content
        ctype = r.headers.get("Content-Type", "")

        if "application/zip" in ctype:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    frames.append(_parse_a85_xml(zf.read(name), tz_out=tz))
        else:
            frames.append(_parse_a85_xml(content, tz_out=tz))

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out
