# backend/app/routers/promotions.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import or_, and_

from ..db import SessionLocal
from ..models import Promotion

router = APIRouter(prefix="/api/v1/promotions", tags=["promotions"])
TZ = ZoneInfo("America/Sao_Paulo")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/today")
def get_today_promotions(db: Session = Depends(get_db)):
    now = datetime.now(TZ)
    # pega promos com valid_until NULL (ambíguo) ou que ainda não expiraram; e sem expired=True
    promos = db.query(Promotion).filter(
        or_(Promotion.valid_until == None, Promotion.valid_until >= now)
    ).order_by(Promotion.date_published.desc()).all()
    return [p.to_dict() for p in promos]

@router.get("/{promo_id}")
def get_promotion(promo_id: int, db: Session = Depends(get_db)):
    promo = db.query(Promotion).filter(Promotion.id == promo_id).first()
    if not promo:
        raise HTTPException(status_code=404, detail="Promoção não encontrada")
    return promo.to_dict()
