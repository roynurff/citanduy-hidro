from flask import Blueprint, current_app, jsonify, request, render_template
from flask_login import login_required
from app.models import UserQuery

from app import limiter
from app.ai_agent import ask

bp = Blueprint("ai", __name__, url_prefix="/ai")


@bp.route("/chat", methods=["POST"])
@limiter.limit("10 per minute")
@login_required
def chat():
    data = request.get_json(silent=True) or {}
    pertanyaan = (data.get("pertanyaan") or "").strip()
    if not pertanyaan:
        return jsonify(error="Pertanyaan kosong."), 400
    if len(pertanyaan) > 500:
        return jsonify(error="Pertanyaan terlalu panjang (maks 500 karakter)."), 400
    try:
        UserQuery.create(q=pertanyaan, intent="llm")
    except Exception:
        current_app.logger.exception("gagal mencatat pertanyaan")
    try:
        return jsonify(jawaban=ask(pertanyaan))
    except Exception:
        current_app.logger.exception("ai chat gagal")
        return jsonify(error="Asisten sedang bermasalah, coba lagi nanti."), 502


@bp.route("/")
@login_required
def halaman():
    return render_template("ai/chat.html")