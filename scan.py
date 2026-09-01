#!/usr/bin/env python3
"""Un tour de scan : quels tokens viennent de naitre avec une image DEJA utilisee.

Pas de statistique, pas de seuil d'ATH : la seule question posee est
« cette image a-t-elle deja servi a un autre token ? ».

Pourquoi un scan periodique et pas un veilleur permanent : pump.fun renvoie 403
sur tout appel portant un en-tete Origin, donc une page HTML ne peut pas
interroger l'API depuis le navigateur. Le scan tourne donc dans GitHub Actions
et publie un fichier JSON que la page lit en meme origine.
"""
import concurrent.futures as cf
import json, os, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import pump_reuse_watch as P

STATE = os.path.join(HERE, "state")
DOCS = os.path.join(HERE, "docs")
DATA = os.path.join(STATE, "data.json")
PRINTS = os.path.join(STATE, "img_prints.json")

PAGES_NEUFS = 2        # 2 x 70 tokens ~= 7 min de creations (absorbe le retard du cron)
PAGES_HOMONYMES = 2    # 140 homonymes tries par ATH : assez pour retrouver la source
SONDES = 4             # homonymes dont on compare l'image
GARDE = 300            # entrees conservees dans le journal public
WORKERS = 16           # tout est reseau : le parallelisme paie
BUDGET = 200.0         # s ; au-dela on s'arrete et on DIT combien on a laisse
# La recherche /coins encaisse les rafales (34 req/s sans 429 mesure) ; c'est
# /coins/{mint} qui bloque l'IP. Le scan n'utilise que /coins.
P.API_MIN_GAP = 0.08
P.MAX_PAGES = PAGES_HOMONYMES


def charger():
    os.makedirs(STATE, exist_ok=True)
    if os.path.exists(PRINTS):
        try:
            P._img_prints.update(json.load(open(PRINTS)))
        except Exception:
            pass
    P.HASH_CACHE, P.DATA = PRINTS, STATE
    try:
        return json.load(open(DATA))
    except Exception:
        return []


def neufs():
    out, vus = [], set()
    for page in range(PAGES_NEUFS):
        try:
            d = P._coins(P.api("/coins", limit=P.PAGE, includeNsfw="true",
                               sort="created_timestamp", order="DESC", offset=page * P.PAGE))
        except Exception as e:
            print(f"  page {page} en echec : {e}", file=sys.stderr)
            break
        if not d:
            break
        for c in d:
            if c.get("mint") and c["mint"] not in vus:
                vus.add(c["mint"]); out.append(c)
        if len(d) < P.PAGE:
            break
    return out


def examiner(c):
    """La question unique : l'image de ce token a-t-elle deja servi ?"""
    mint, sym = c.get("mint"), (c.get("symbol") or c.get("name") or "")
    if not mint or not sym:
        return None
    try:
        jumeaux, _ = P.clones_of(sym)
    except Exception:
        return None
    naissance = (c.get("created_timestamp") or 0) / 1000
    # la source doit exister AVANT : sinon ce n'est pas une reutilisation
    plus_vieux = [t for t in jumeaux
                  if t.get("mint") != mint and 0 < (t.get("created_timestamp") or 0) / 1000 < naissance]
    if not plus_vieux:
        return None
    # l'image du candidat n'est telechargee que s'il y a quelque chose a comparer
    fa = P.img_print(c.get("image_uri"))
    if fa is None:
        return None
    for t in sorted(plus_vieux, key=lambda t: -P.ath(t))[:SONDES]:
        t_ts = (t.get("created_timestamp") or 0) / 1000
        fb = P.img_print(t.get("image_uri"))
        if fb is None:
            continue
        pareil, dist = P.same_image(fa, fb)
        if pareil:
            return {
                "mint": mint, "symbol": sym, "cree": naissance,
                "image": c.get("image_uri"),
                "src_mint": t.get("mint"), "src_symbol": t.get("symbol") or t.get("name"),
                "src_cree": t_ts, "src_ath": P.ath(t), "src_image": t.get("image_uri"),
                "jours": round((naissance - t_ts) / 86400, 1),
                "dist": dist, "n_homonymes": len(jumeaux),
                "lanceur": P.launcher_of(c.get("metadata_uri")),
                "vu": time.time(),
            }
    return None


def main():
    journal = charger()
    connus = {e["mint"] for e in journal}
    lot = [c for c in neufs() if c["mint"] not in connus]
    t0 = time.time()
    trouves, faits = [], 0
    with cf.ThreadPoolExecutor(WORKERS) as ex:
        futs = {ex.submit(examiner, c): c for c in lot}
        for f in cf.as_completed(futs, timeout=None):
            faits += 1
            try:
                r = f.result()
            except Exception:
                r = None
            if r:
                trouves.append(r)
            if time.time() - t0 > BUDGET:
                for autre in futs:
                    autre.cancel()
                break
    ignores = len(lot) - faits
    if ignores > 0:
        print(f"  budget de {BUDGET:.0f}s atteint : {ignores} tokens NON examines "
              f"(ils reviendront au tour suivant s'ils sont encore dans la fenetre)")
    journal = (trouves + journal)[:GARDE]
    journal.sort(key=lambda e: -e["cree"])

    os.makedirs(DOCS, exist_ok=True)
    json.dump(journal, open(DATA, "w"))
    json.dump({"maj": time.time(), "scannes": faits, "ignores": max(ignores, 0),
               "trouves": len(trouves), "entrees": journal},
              open(os.path.join(DOCS, "data.json"), "w"))
    P.save_cache()
    print(f"{faits}/{len(lot)} tokens examines en {time.time() - t0:.0f}s, "
          f"{len(trouves)} images reutilisees, journal={len(journal)}")
    for r in trouves:
        print(f"  {r['symbol']:<14} copie {r['src_symbol']} (il y a {r['jours']}j, "
              f"ATH ${r['src_ath']:,.0f}) d={r['dist']}")


if __name__ == "__main__":
    main()
