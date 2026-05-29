from flask import Flask, request, jsonify

app = Flask(__name__)

counters = {
    "total": 0,
    "high": 0,
    "critical": 0
}

def score_transaction(tx):
    score = 0
    rules = []

    if tx.get("amount", 0) > 3000:
        score += 3
        rules.append("R1: amount > 3000")

    if tx.get("category") == "electronics" and tx.get("amount", 0) > 1500:
        score += 2
        rules.append("R2: electronics > 1500")

    if tx.get("hour", 12) < 6:
        score += 2
        rules.append("R3: night hour")

    if score >= 5:
        risk_level = "CRITICAL"
    elif score >= 3:
        risk_level = "HIGH"
    elif score >= 1:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    return {
        "score": score,
        "risk_level": risk_level,
        "triggered_rules": rules
    }

@app.route("/score", methods=["POST"])
def score():
    tx = request.get_json()

    if not tx or "amount" not in tx:
        return jsonify({
            "error": "Missing required field 'amount'"
        }), 400

    if tx["amount"] < 0:
        return jsonify({
            "error": "Amount cannot be negative"
        }), 400

    result = score_transaction(tx)
    result["tx_id"] = tx.get("tx_id", "unknown")

    counters["total"] += 1

    if result["risk_level"] == "HIGH":
        counters["high"] += 1

    if result["risk_level"] == "CRITICAL":
        counters["critical"] += 1

    return jsonify(result)

@app.route("/stats")
def stats():
    return jsonify(counters)

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "version": "1.0-rules"
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
