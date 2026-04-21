# CLAUDE.md

## 🧭 SYSTEM IDENTITY
You are working on a high-performance Kalshi trading system.
This is a precision system — NOT experimental, NOT exploratory.

All changes must:
- Preserve execution integrity
- Maintain profitability focus
- Avoid unnecessary complexity

Do NOT rewrite large sections unless explicitly asked.

---

## ⚙️ CORE ARCHITECTURE RULES

- Language: Python (asyncio + aiohttp)
- Execution must remain asynchronous and non-blocking
- Do NOT introduce sync calls or blocking loops
- Maintain current pipeline structure

STRICTLY USE:
https://api.kalshi.com/trade-api/v2

DO NOT USE:
- elections subdomain
- batch orderbook endpoints
- deprecated endpoints

---

## 📊 MARKET DATA RULES

- Always use per-ticker orderbook:
  /markets/{ticker}/orderbook

- Extract:
  - yes_bid / no_bid
  - yes_ask / no_ask
  - liquidity (touch size)

- If orderbook is incomplete → REJECT market

- Never assume missing data
- Never fabricate prices

---

## 🎯 STRATEGY IDENTITY

Primary strategy:
→ NO-side, low price (1¢–10¢ range)

System philosophy:
- We are pricing inefficiency, NOT predicting direction
- Edge comes from mispriced tails

---

## 🚫 ENTRY RULES (STRICT)

Reject trade if ANY condition fails:

- pressure < 0.48 → REJECT
- abs(model_prob - 0.50) < 0.08 → NO CLEAR EDGE → REJECT
- spread > 0.07 → REJECT
- illiquid book (low touch size) → REJECT

ONLY TAKE:
- Clear directional bias
- Tight spread
- Real liquidity

---

## 📈 EDGE & CONVICTION

- Edge must be REAL and executable
- No theoretical edge without liquidity

Conviction tiers:
- elite
- strong
- neutral
- weak

Do NOT upgrade conviction artificially

---

## 🧠 EXECUTION PHILOSOPHY

- Do NOT overtrade
- Quality > quantity
- Fewer high-quality trades beat many weak ones

Avoid:
- dead markets
- weak pressure setups
- noisy signals

---

## ⛔ EXIT RULES (CRITICAL)

DO NOT exit early unless necessary

Long-horizon trades:
- If hours_to_close > 1h AND entry_edge > 0.20
  → HOLD TO EXPIRY
  → DO NOT ROTATE
  → DO NOT TIME-STOP

Active exits allowed:
- hard_stop
- catastrophic failure
- severe model breakdown

Protect winners:
- If pnl > 2% → trail stop

---

## 💰 RISK MANAGEMENT

- Respect bankroll at all times
- Max loss per trade must be enforced
- Never exceed exposure limits

Do NOT:
- stack correlated positions
- over-allocate on weak signals

---

## 📊 LOGGING RULES

Logs must be:

- Clean
- Minimal
- Actionable

PRIORITY:
- Entry decisions
- Exit reasons
- PnL tracking

REMOVE:
- noise
- redundant debug spam
- unused metrics

---

## 🧪 DEVELOPMENT RULES

When modifying code:

- Return DIFF-style patches (not full rewrites)
- Explain WHY change improves system
- Do NOT introduce breaking changes

Before suggesting changes:
- Identify root cause
- Validate against strategy rules

---

## 🚨 HARD CONSTRAINTS

NEVER:

- Break execution loop
- Introduce fake data
- Ignore liquidity
- Trade without edge
- Override risk controls

---

## 🧠 MINDSET

This is a disciplined trading system.

Not:
- a toy
- a random bot
- a guessing engine

Every trade must have:
→ logic  
→ edge  
→ execution quality  

If not → DO NOT TRADE
