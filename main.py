import asyncio
import httpx
import os
from typing import Dict, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, Session

DATABASE_URL = "sqlite:///./trading.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    school = Column(String, index=True, default="일반")
    cash_balance = Column(Float, default=10000000.0)

class Position(Base):
    __tablename__ = "positions"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    ticker = Column(String, index=True)
    quantity = Column(Integer, default=0)
    total_cost = Column(Float, default=0.0)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 부산 전역 학교 및 전국 주요교 데이터베이스
KOREAN_SCHOOLS = [
    # 대학교
    {"name": "부산대학교", "type": "대학교", "region": "부산 금정구"},
    {"name": "부경대학교", "type": "대학교", "region": "부산 남구"},
    {"name": "한국해양대학교", "type": "대학교", "region": "부산 영도구"},
    {"name": "동아대학교", "type": "대학교", "region": "부산 사하구/서구"},
    {"name": "경성대학교", "type": "대학교", "region": "부산 남구"},
    {"name": "동의대학교", "type": "대학교", "region": "부산 부산진구"},
    {"name": "동서대학교", "type": "대학교", "region": "부산 사상구"},
    {"name": "신라대학교", "type": "대학교", "region": "부산 사상구"},
    {"name": "부산외국어대학교", "type": "대학교", "region": "부산 금정구"},
    {"name": "부산교육대학교", "type": "대학교", "region": "부산 연제구"},
    {"name": "서울대학교", "type": "대학교", "region": "서울 관악구"},
    {"name": "연세대학교", "type": "대학교", "region": "서울 서대문구"},
    {"name": "고려대학교", "type": "대학교", "region": "서울 성북구"},
    {"name": "KAIST (한국과학기술원)", "type": "대학교", "region": "대전 유성구"},
    {"name": "POSTECH (포항공과대학교)", "type": "대학교", "region": "경북 포항시"},
    # 고등학교
    {"name": "한국과학영재학교", "type": "고등학교", "region": "부산 부산진구"},
    {"name": "부산과학고등학교", "type": "고등학교", "region": "부산 금정구"},
    {"name": "부산일과학고등학교", "type": "고등학교", "region": "부산 사하구"},
    {"name": "부산국제고등학교", "type": "고등학교", "region": "부산 부산진구"},
    {"name": "부산외국어고등학교", "type": "고등학교", "region": "부산 연제구"},
    {"name": "해운대고등학교", "type": "고등학교", "region": "부산 해운대구"},
    {"name": "사직고등학교", "type": "고등학교", "region": "부산 동래구"},
    {"name": "부산고등학교", "type": "고등학교", "region": "부산 동구"},
    {"name": "경남고등학교", "type": "고등학교", "region": "부산 서구"},
    {"name": "대연고등학교", "type": "고등학교", "region": "부산 남구"},
    {"name": "명호고등학교", "type": "고등학교", "region": "부산 강서구"},
    # 중학교 (부산 16개 구군 전역)
    {"name": "센텀중학교", "type": "중학교", "region": "부산 해운대구"},
    {"name": "해운대중학교", "type": "중학교", "region": "부산 해운대구"},
    {"name": "해운대여자중학교", "type": "중학교", "region": "부산 해운대구"},
    {"name": "동백중학교", "type": "중학교", "region": "부산 해운대구"},
    {"name": "신곡중학교", "type": "중학교", "region": "부산 해운대구"},
    {"name": "부흥중학교", "type": "중학교", "region": "부산 해운대구"},
    {"name": "양운중학교", "type": "중학교", "region": "부산 해운대구"},
    {"name": "상당중학교", "type": "중학교", "region": "부산 해운대구"},
    {"name": "여명중학교", "type": "중학교", "region": "부산 동래구"},
    {"name": "동래중학교", "type": "중학교", "region": "부산 동래구"},
    {"name": "사직중학교", "type": "중학교", "region": "부산 동래구"},
    {"name": "유락여자중학교", "type": "중학교", "region": "부산 동래구"},
    {"name": "남천중학교", "type": "중학교", "region": "부산 수영구"},
    {"name": "수영중학교", "type": "중학교", "region": "부산 수영구"},
    {"name": "대연중학교", "type": "중학교", "region": "부산 남구"},
    {"name": "분포중학교", "type": "중학교", "region": "부산 남구"},
    {"name": "오륙도중학교", "type": "중학교", "region": "부산 남구"},
    {"name": "서면중학교", "type": "중학교", "region": "부산 부산진구"},
    {"name": "구서중학교", "type": "중학교", "region": "부산 금정구"},
    {"name": "화명중학교", "type": "중학교", "region": "부산 북구"},
    {"name": "명진중학교", "type": "중학교", "region": "부산 북구"},
    {"name": "주례중학교", "type": "중학교", "region": "부산 사상구"},
    {"name": "하단중학교", "type": "중학교", "region": "부산 사하구"},
    {"name": "당리중학교", "type": "중학교", "region": "부산 사하구"},
    {"name": "명호중학교", "type": "중학교", "region": "부산 강서구"},
    {"name": "명지중학교", "type": "중학교", "region": "부산 강서구"},
    {"name": "정관중학교", "type": "중학교", "region": "부산 기장군"},
    {"name": "대치중학교", "type": "중학교", "region": "서울 강남구"},
    {"name": "목운중학교", "type": "중학교", "region": "서울 양천구"}
]

async def fetch_real_stock_price(ticker: str) -> int:
    url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(url, headers=headers, timeout=3.0)
            if res.status_code == 200:
                data = res.json()
                raw_price = data["datas"][0]["closePrice"].replace(",", "")
                return int(raw_price)
    except Exception:
        pass
    return None

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.user_connections: Dict[int, WebSocket] = {}

    async def connect(self, websocket: WebSocket, user_id: int = None):
        await websocket.accept()
        self.active_connections.append(websocket)
        if user_id:
            self.user_connections[user_id] = websocket

    def disconnect(self, websocket: WebSocket, user_id: int = None):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if user_id and user_id in self.user_connections:
            del self.user_connections[user_id]

    async def broadcast_market_data(self, data: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(data)
            except Exception:
                pass

    async def send_personal_message(self, message: dict, user_id: int):
        if user_id in self.user_connections:
            try:
                await self.user_connections[user_id].send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

TICKERS = {"005930": "삼성전자", "035420": "NAVER", "035720": "카카오", "000660": "SK하이닉스"}
CURRENT_MARKET_PRICES = {"005930": 71000, "035420": 208000, "035720": 53000, "000660": 158000}

async def market_data_loop():
    while True:
        for ticker in TICKERS.keys():
            price = await fetch_real_stock_price(ticker)
            if price:
                CURRENT_MARKET_PRICES[ticker] = price
        
        await manager.broadcast_market_data({
            "type": "PRICE_UPDATE",
            "data": CURRENT_MARKET_PRICES
        })
        await asyncio.sleep(3)

def init_seed_data():
    db = SessionLocal()
    try:
        if db.query(User).count() == 0:
            sample_users = [
                {"id": 1, "username": "해운대고수", "school": "센텀중학교", "cash": 7200000.0, "stocks": [("005930", 40, 68000.0)]},
                {"id": 2, "username": "부산대버핏", "school": "부산대학교", "cash": 4500000.0, "stocks": [("000660", 35, 148000.0)]},
                {"id": 3, "username": "남천동큰손", "school": "남천중학교", "cash": 8500000.0, "stocks": [("035720", 30, 51000.0)]},
                {"id": 4, "username": "동래학사", "school": "여명중학교", "cash": 3600000.0, "stocks": [("000660", 40, 145000.0)]},
                {"id": 5, "username": "명지스탁", "school": "명호중학교", "cash": 5000000.0, "stocks": [("005930", 70, 69500.0)]}
            ]
            for u_data in sample_users:
                u = User(id=u_data["id"], username=u_data["username"], school=u_data["school"], cash_balance=u_data["cash"])
                db.add(u)
                db.flush()
                for ticker, qty, cost in u_data["stocks"]:
                    pos = Position(user_id=u.id, ticker=ticker, quantity=qty, total_cost=cost * qty)
                    db.add(pos)
            db.commit()
    finally:
        db.close()

app = FastAPI(title="Toss Campus Trading System")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    init_seed_data()
    asyncio.create_task(market_data_loop())

class LoginRequest(BaseModel):
    username: str
    school: str

class OrderRequest(BaseModel):
    user_id: int
    ticker: str
    quantity: int

@app.get("/schools/search")
def search_schools(q: str = Query("", description="검색할 학교 이름")):
    keyword = q.strip().lower()
    if not keyword:
        return [s for s in KOREAN_SCHOOLS if "부산" in s["region"]][:10]
    results = [s for s in KOREAN_SCHOOLS if keyword in s["name"].lower() or keyword in s["region"].lower()]
    return results[:20]

@app.post("/auth/login")
def login_or_register(req: LoginRequest, db: Session = Depends(get_db)):
    clean_username = req.username.strip()
    clean_school = req.school.strip() if req.school.strip() else "일반"
    if not clean_username:
        raise HTTPException(status_code=400, detail="이름을 입력해주세요.")

    user = db.query(User).filter(User.username == clean_username).first()
    if not user:
        user = User(username=clean_username, school=clean_school, cash_balance=10000000.0)
        db.add(user)
        db.commit()
        db.refresh(user)
    else:
        if clean_school and clean_school != "일반":
            user.school = clean_school
            db.commit()
            db.refresh(user)

    return {
        "user_id": user.id,
        "username": user.username,
        "school": user.school,
        "cash_balance": user.cash_balance
    }

@app.get("/portfolio/{user_id}")
def get_portfolio(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="유저 없음")

    positions = db.query(Position).filter(Position.user_id == user_id, Position.quantity > 0).all()
    total_stock_value = sum([p.quantity * CURRENT_MARKET_PRICES.get(p.ticker, 0) for p in positions])
    total_asset = user.cash_balance + total_stock_value
    return_rate = ((total_asset - 10000000.0) / 10000000.0) * 100.0

    return {
        "user_id": user.id,
        "username": user.username,
        "school": user.school,
        "cash_balance": user.cash_balance,
        "stock_value": total_stock_value,
        "total_asset": total_asset,
        "return_rate": round(return_rate, 2),
        "positions": [
            {
                "ticker": p.ticker,
                "quantity": p.quantity,
                "avg_price": (p.total_cost / p.quantity) if p.quantity > 0 else 0,
                "total_cost": p.total_cost,
                "current_price": CURRENT_MARKET_PRICES.get(p.ticker, 0)
            }
            for p in positions
        ]
    }

@app.post("/order/buy")
async def buy_order(order: OrderRequest, db: Session = Depends(get_db)):
    current_price = CURRENT_MARKET_PRICES.get(order.ticker)
    if not current_price:
        raise HTTPException(status_code=400, detail="유효하지 않은 종목")

    with db.begin():
        user = db.query(User).filter(User.id == order.user_id).with_for_update().first()
        total_price = order.quantity * current_price
        if user.cash_balance < total_price:
            raise HTTPException(status_code=400, detail="예수금 부족")

        user.cash_balance -= total_price
        pos = db.query(Position).filter(Position.user_id == order.user_id, Position.ticker == order.ticker).first()
        if not pos:
            pos = Position(user_id=order.user_id, ticker=order.ticker, quantity=0, total_cost=0.0)
            db.add(pos)

        pos.quantity += order.quantity
        pos.total_cost += total_price

    await manager.send_personal_message({
        "type": "ORDER_EXECUTION",
        "data": f"[{TICKERS.get(order.ticker, order.ticker)}] {order.quantity}주 매수 체결"
    }, user_id=order.user_id)

    return {"status": "success"}

@app.post("/order/sell")
async def sell_order(order: OrderRequest, db: Session = Depends(get_db)):
    current_price = CURRENT_MARKET_PRICES.get(order.ticker)
    with db.begin():
        user = db.query(User).filter(User.id == order.user_id).with_for_update().first()
        pos = db.query(Position).filter(Position.user_id == order.user_id, Position.ticker == order.ticker).with_for_update().first()

        if not pos or pos.quantity < order.quantity:
            raise HTTPException(status_code=400, detail="보유 수량 부족")

        avg_price = pos.total_cost / pos.quantity
        cost_basis_sold = avg_price * order.quantity
        proceeds = order.quantity * current_price

        pos.quantity -= order.quantity
        pos.total_cost -= cost_basis_sold
        user.cash_balance += proceeds

    await manager.send_personal_message({
        "type": "ORDER_EXECUTION",
        "data": f"[{TICKERS.get(order.ticker, order.ticker)}] {order.quantity}주 매도 체결"
    }, user_id=order.user_id)

    return {"status": "success"}

@app.get("/rankings/users")
def get_user_rankings(db: Session = Depends(get_db)):
    users = db.query(User).all()
    ranking_list = []
    for u in users:
        positions = db.query(Position).filter(Position.user_id == u.id, Position.quantity > 0).all()
        stock_eval = sum([p.quantity * CURRENT_MARKET_PRICES.get(p.ticker, 0) for p in positions])
        total = u.cash_balance + stock_eval
        rate = ((total - 10000000.0) / 10000000.0) * 100.0
        ranking_list.append({
            "username": u.username,
            "school": u.school,
            "total_asset": total,
            "return_rate": round(rate, 2)
        })
    ranking_list.sort(key=lambda x: x["return_rate"], reverse=True)
    for idx, item in enumerate(ranking_list):
        item["rank"] = idx + 1
    return ranking_list

@app.get("/rankings/schools")
def get_school_rankings(db: Session = Depends(get_db)):
    users = db.query(User).all()
    school_map = {}
    for u in users:
        positions = db.query(Position).filter(Position.user_id == u.id, Position.quantity > 0).all()
        stock_eval = sum([p.quantity * CURRENT_MARKET_PRICES.get(p.ticker, 0) for p in positions])
        total = u.cash_balance + stock_eval
        rate = ((total - 10000000.0) / 10000000.0) * 100.0

        school = u.school if u.school else "기타"
        if school not in school_map:
            school_map[school] = {"total_rate": 0.0, "total_asset": 0.0, "count": 0}
        school_map[school]["total_rate"] += rate
        school_map[school]["total_asset"] += total
        school_map[school]["count"] += 1

    school_list = []
    for school_name, data in school_map.items():
        avg_rate = data["total_rate"] / data["count"]
        avg_asset = data["total_asset"] / data["count"]
        school_list.append({
            "school": school_name,
            "avg_return_rate": round(avg_rate, 2),
            "avg_asset": avg_asset,
            "participant_count": data["count"]
        })
    school_list.sort(key=lambda x: x["avg_return_rate"], reverse=True)
    for idx, item in enumerate(school_list):
        item["rank"] = idx + 1
    return school_list

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await manager.connect(websocket, user_id)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)

# ---------------------------------------------------------

# ---------------------------------------------------------
# 태블릿 및 단일 파일 배포용 HTML 내장 서빙 (폴더 불필요)
# ---------------------------------------------------------
from fastapi.responses import HTMLResponse

HTML_TEMPLATE = r"""<!doctype html>
<html lang="ko">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
    <title>토스 캠퍼스 모의투자 리그</title>
    <!-- Pretendard Font -->
    <link rel="stylesheet" as="style" crossorigin href="https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/variable/pretendardvariable.min.css" />
    <!-- Tailwind CSS CDN -->
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
      tailwind.config = {
        theme: {
          extend: {
            colors: {
              tossbg: '#F2F4F6',
              tosstext: '#191F28',
              tosssub: '#8B95A1',
              tossred: '#F04452',
              tossblue: '#3182F6',
              tosswhite: '#FFFFFF'
            },
            borderRadius: {
              '2xl': '16px',
              '3xl': '24px'
            }
          }
        }
      }
    </script>
    <!-- React & Babel for zero-build browser rendering -->
    <script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
    <script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
    <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
    <!-- Lucide Icons -->
    <script src="https://unpkg.com/lucide@latest"></script>

    <style>
      body {
        background-color: #F2F4F6;
        font-family: 'Pretendard Variable', Pretendard, -apple-system, BlinkMacSystemFont, system-ui, Roboto, sans-serif;
        color: #191F28;
      }
      input::-webkit-outer-spin-button,
      input::-webkit-inner-spin-button {
        -webkit-appearance: none;
        margin: 0;
      }
      @keyframes fadeInDown {
        from { opacity: 0; transform: translateY(-12px); }
        to { opacity: 1; transform: translateY(0); }
      }
      .animate-fade-in-down {
        animation: fadeInDown 0.25s ease-out;
      }
    </style>
  </head>
  <body>
    <div id="root"></div>

    <script type="text/babel">
      const { useState, useEffect } = React;

      // 동적 호스트 설정 (로컬 및 Railway 어디서든 자동 인식)
      const isHttps = window.location.protocol === 'https:';
      const API_BASE = window.location.origin;
      const WS_PROTOCOL = isHttps ? 'wss:' : 'ws:';
      const WS_HOST = window.location.host;

      const STOCK_NAMES = { "005930": "삼성전자", "035420": "NAVER", "035720": "카카오", "000660": "SK하이닉스" };

      function App() {
        const [currentUser, setCurrentUser] = useState(() => {
          const saved = localStorage.getItem("toss_trade_user");
          return saved ? JSON.parse(saved) : { user_id: 1, username: "해운대고수", school: "센텀중학교" };
        });

        const [showLoginModal, setShowLoginModal] = useState(false);
        const [inputName, setInputName] = useState("");
        const [inputSchool, setInputSchool] = useState("");
        
        const [schoolSearchResults, setSchoolSearchResults] = useState([]);
        const [portfolio, setPortfolio] = useState({ cash_balance: 0, total_asset: 0, return_rate: 0, positions: [], school: '' });
        const [prices, setPrices] = useState({});
        const [prevPrices, setPrevPrices] = useState({});
        
        const [rankingTab, setRankingTab] = useState('SCHOOL');
        const [userRankings, setUserRankings] = useState([]);
        const [schoolRankings, setSchoolRankings] = useState([]);

        const [selectedTicker, setSelectedTicker] = useState("005930");
        const [quantity, setQuantity] = useState("");
        const [orderType, setOrderType] = useState('BUY');
        const [toastMessage, setToastMessage] = useState("");

        const showToast = (msg) => {
          setToastMessage(msg);
          setTimeout(() => setToastMessage(""), 3000);
        };

        // 실시간 학교 검색
        useEffect(() => {
          if (!showLoginModal) return;
          const fetchSchools = async () => {
            try {
              const res = await fetch(`${API_BASE}/schools/search?q=${encodeURIComponent(inputSchool)}`);
              if (res.ok) {
                const data = await res.json();
                setSchoolSearchResults(data);
              }
            } catch (e) {}
          };
          const timer = setTimeout(fetchSchools, 200);
          return () => clearTimeout(timer);
        }, [inputSchool, showLoginModal]);

        const fetchData = async () => {
          if (!currentUser?.user_id) return;
          try {
            const pRes = await fetch(`${API_BASE}/portfolio/${currentUser.user_id}`);
            if (pRes.ok) setPortfolio(await pRes.json());

            const uRes = await fetch(`${API_BASE}/rankings/users`);
            if (uRes.ok) setUserRankings(await uRes.json());

            const sRes = await fetch(`${API_BASE}/rankings/schools`);
            if (sRes.ok) setSchoolRankings(await sRes.json());
          } catch (e) {}
        };

        useEffect(() => {
          fetchData();
          const ws = new WebSocket(`${WS_PROTOCOL}//${WS_HOST}/ws/${currentUser.user_id}`);
          ws.onmessage = (event) => {
            const response = JSON.parse(event.data);
            if (response.type === "PRICE_UPDATE") {
              setPrices(curr => { setPrevPrices(curr); return response.data; });
            } else if (response.type === "ORDER_EXECUTION") {
              showToast(response.data);
              fetchData();
            }
          };
          const interval = setInterval(fetchData, 4000);
          return () => { ws.close(); clearInterval(interval); };
        }, [currentUser]);

        const handleLogin = async (e) => {
          e.preventDefault();
          if (!inputName.trim()) return alert("이름(닉네임)을 입력해주세요.");
          const finalSchool = inputSchool.trim() || "일반";
          try {
            const res = await fetch(`${API_BASE}/auth/login`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                username: inputName.trim(),
                school: finalSchool
              })
            });
            if (res.ok) {
              const data = await res.json();
              setCurrentUser(data);
              localStorage.setItem("toss_trade_user", JSON.stringify(data));
              setShowLoginModal(false);
              showToast(`환영합니다! ${data.school} ${data.username}님`);
            }
          } catch (e) {
            alert("로그인 처리 실패");
          }
        };

        const handleOrder = async () => {
          if (!quantity || quantity <= 0) return alert("수량을 입력해주세요.");
          const endpoint = orderType === 'BUY' ? '/order/buy' : '/order/sell';
          try {
            const res = await fetch(`${API_BASE}${endpoint}`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                user_id: currentUser.user_id,
                ticker: selectedTicker,
                quantity: parseInt(quantity)
              })
            });
            if (!res.ok) {
              const error = await res.json();
              alert(error.detail);
            } else {
              setQuantity("");
              fetchData();
            }
          } catch (e) { alert("서버 통신 실패"); }
        };

        const getPriceColor = (t, currentP) => {
          const prev = prevPrices[t];
          if (!prev || currentP === prev) return "text-tosstext";
          return currentP > prev ? "text-tossred" : "text-tossblue";
        };

        const currentPrice = prices[selectedTicker] || 0;
        const estimatedTotal = currentPrice * (parseInt(quantity) || 0);

        return (
          <div className="max-w-[1020px] mx-auto min-h-screen pb-24 md:py-10 md:px-6">
            
            {toastMessage && (
              <div className="fixed top-5 left-1/2 -translate-x-1/2 z-[100] bg-slate-900 text-white px-5 py-3 rounded-full text-sm font-medium shadow-xl flex items-center gap-2 animate-bounce">
                <span>✓</span>
                <span>{toastMessage}</span>
              </div>
            )}

            {/* 헤더 */}
            <header className="flex justify-between items-center px-4 py-3 md:px-0 mb-4">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 bg-tossblue rounded-xl flex items-center justify-center text-white font-extrabold text-sm">
                  T
                </div>
                <span className="font-extrabold text-xl text-tosstext">캠퍼스 모의투자 리그</span>
              </div>
              <button 
                onClick={() => {
                  setInputName(currentUser.username || "");
                  setInputSchool(currentUser.school || "");
                  setShowLoginModal(true);
                }}
                className="flex items-center gap-2 bg-tosswhite px-4 py-2 rounded-full border border-slate-200 text-sm font-semibold text-slate-700 shadow-sm hover:bg-slate-50 transition"
              >
                <span>🏫</span>
                <span>{portfolio.school || currentUser.school || "학교 검색"}</span>
                <span className="text-slate-300">|</span>
                <span>{currentUser.username}님</span>
              </button>
            </header>

            <div className="flex flex-col lg:flex-row gap-6">
              
              {/* 왼쪽 컬럼 : 내 자산 + 시세 + 랭킹 */}
              <div className="flex-1 space-y-4 px-4 md:px-0">
                
                {/* 자산 요약 카드 */}
                <div className="bg-tosswhite p-6 md:rounded-3xl shadow-sm border border-slate-100">
                  <div className="flex justify-between items-center mb-3">
                    <div className="flex items-center gap-2">
                      <span className="bg-blue-50 text-tossblue px-2.5 py-1 rounded-lg text-xs font-bold">
                        🏫 {portfolio.school || currentUser.school}
                      </span>
                      <span className="font-semibold text-slate-600 text-sm">{currentUser.username}님의 총 자산</span>
                    </div>
                    <span className={`text-sm font-bold ${portfolio.return_rate >= 0 ? 'text-tossred' : 'text-tossblue'}`}>
                      {portfolio.return_rate > 0 ? '+' : ''}{portfolio.return_rate}%
                    </span>
                  </div>
                  
                  <div className="text-4xl font-extrabold mb-3 tracking-tight">{portfolio.total_asset.toLocaleString()}원</div>
                  <div className="text-tosssub text-xs md:text-sm flex gap-2">
                    <span>주식 {((portfolio.total_asset || 0) - (portfolio.cash_balance || 0)).toLocaleString()}원</span>
                    <span>•</span>
                    <span>주문가능 예수금 {portfolio.cash_balance.toLocaleString()}원</span>
                  </div>
                </div>

                {/* 실시간 시세 리스트 */}
                <div className="bg-tosswhite p-6 md:rounded-3xl shadow-sm border border-slate-100">
                  <h2 className="text-xl font-bold mb-4 flex justify-between items-center">
                    실시간 국내 주식
                    <span className="text-xs font-normal text-tosssub bg-slate-100 px-2 py-1 rounded-md">네이버 실시간 시세</span>
                  </h2>
                  <div className="flex flex-col divide-y divide-slate-100">
                    {Object.entries(prices).map(([t, p]) => (
                      <div 
                        key={t} onClick={() => setSelectedTicker(t)}
                        className={`flex justify-between items-center py-3.5 px-3 rounded-2xl cursor-pointer transition ${selectedTicker === t ? 'bg-blue-50/60' : 'hover:bg-slate-50'}`}
                      >
                        <div className="flex items-center gap-3.5">
                          <div className="w-11 h-11 bg-tossbg rounded-2xl flex items-center justify-center font-bold text-tosssub text-xs">
                            {STOCK_NAMES[t].substring(0, 2)}
                          </div>
                          <div>
                            <div className="font-bold text-[16px] text-tosstext">{STOCK_NAMES[t]}</div>
                            <div className="text-tosssub text-xs">{t}</div>
                          </div>
                        </div>
                        <div className="text-right">
                          <div className={`font-bold text-[16px] ${getPriceColor(t, p)}`}>{p.toLocaleString()}원</div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* 랭킹 보드 */}
                <div className="bg-tosswhite p-6 md:rounded-3xl shadow-sm border border-slate-100">
                  <div className="flex justify-between items-center mb-5">
                    <h2 className="text-xl font-bold flex items-center gap-2">
                      <span>🏆</span> 리그 랭킹
                    </h2>
                    <div className="flex bg-slate-100 p-1 rounded-xl text-xs font-bold">
                      <button 
                        className={`px-3 py-1.5 rounded-lg transition ${rankingTab === 'SCHOOL' ? 'bg-white text-tossblue shadow-sm' : 'text-slate-500'}`}
                        onClick={() => setRankingTab('SCHOOL')}
                      >
                        학교 대항전
                      </button>
                      <button 
                        className={`px-3 py-1.5 rounded-lg transition ${rankingTab === 'INDIVIDUAL' ? 'bg-white text-tossblue shadow-sm' : 'text-slate-500'}`}
                        onClick={() => setRankingTab('INDIVIDUAL')}
                      >
                        개인 랭킹
                      </button>
                    </div>
                  </div>

                  {rankingTab === 'SCHOOL' ? (
                    <div className="space-y-3">
                      {schoolRankings.map((s) => (
                        <div key={s.school} className="flex justify-between items-center p-3.5 rounded-2xl bg-slate-50/80 hover:bg-slate-100 transition">
                          <div className="flex items-center gap-3">
                            <span className={`w-6 text-center font-extrabold text-sm ${s.rank <= 3 ? 'text-tossblue' : 'text-slate-400'}`}>{s.rank}</span>
                            <div>
                              <div className="font-bold text-[15px] flex items-center gap-1.5">
                                <span>🏫</span>
                                <span>{s.school}</span>
                              </div>
                              <div className="text-xs text-tosssub">참가자 {s.participant_count}명</div>
                            </div>
                          </div>
                          <div className="text-right">
                            <div className={`font-bold text-[15px] ${s.avg_return_rate >= 0 ? 'text-tossred' : 'text-tossblue'}`}>
                              {s.avg_return_rate > 0 ? '+' : ''}{s.avg_return_rate}%
                            </div>
                            <div className="text-xs text-slate-400">평균 수익률</div>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {userRankings.map((u) => (
                        <div key={u.username} className="flex justify-between items-center p-3.5 rounded-2xl bg-slate-50/80 hover:bg-slate-100 transition">
                          <div className="flex items-center gap-3">
                            <span className={`w-6 text-center font-extrabold text-sm ${u.rank <= 3 ? 'text-tossblue' : 'text-slate-400'}`}>{u.rank}</span>
                            <div>
                              <div className="font-bold text-[15px]">{u.username}</div>
                              <div className="text-xs text-tosssub">{u.school}</div>
                            </div>
                          </div>
                          <div className="text-right">
                            <div className={`font-bold text-[15px] ${u.return_rate >= 0 ? 'text-tossred' : 'text-tossblue'}`}>
                              {u.return_rate > 0 ? '+' : ''}{u.return_rate}%
                            </div>
                            <div className="text-xs text-slate-400">{Math.round(u.total_asset).toLocaleString()}원</div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

              </div>

              {/* 오른쪽 컬럼 : 주문창 (모바일 하단 고정, PC 사이드바) */}
              <div className="w-full lg:w-[380px] lg:shrink-0 sticky top-6 self-start">
                <div className="bg-tosswhite p-6 md:rounded-3xl shadow-sm border border-slate-100 fixed bottom-0 left-0 right-0 md:relative z-40 rounded-t-3xl md:rounded-none">
                  
                  <div className="flex justify-between items-center mb-5">
                    <div>
                      <div className="text-tosssub text-xs mb-0.5">{selectedTicker}</div>
                      <div className="text-2xl font-bold text-tosstext">{STOCK_NAMES[selectedTicker]}</div>
                    </div>
                    <div className="text-right">
                      <div className="text-2xl font-extrabold">{currentPrice.toLocaleString()}원</div>
                    </div>
                  </div>

                  <div className="flex bg-tossbg rounded-2xl p-1 mb-5">
                    <button 
                      className={`flex-1 py-3 text-[15px] font-bold rounded-xl transition ${orderType === 'BUY' ? 'bg-white text-tossred shadow-sm' : 'text-tosssub'}`}
                      onClick={() => setOrderType('BUY')}
                    >매수</button>
                    <button 
                      className={`flex-1 py-3 text-[15px] font-bold rounded-xl transition ${orderType === 'SELL' ? 'bg-white text-tossblue shadow-sm' : 'text-tosssub'}`}
                      onClick={() => setOrderType('SELL')}
                    >매도</button>
                  </div>

                  <div className="mb-5 relative">
                    <input 
                      type="number" 
                      value={quantity} onChange={(e) => setQuantity(e.target.value)}
                      placeholder="몇 주 주문할까요?"
                      className="w-full text-xl font-bold border-b-2 border-slate-200 focus:border-tossblue outline-none py-3 pr-10 transition bg-transparent placeholder:text-slate-300"
                    />
                    <span className="absolute right-0 top-3 text-lg font-bold text-tosstext">주</span>
                  </div>

                  <div className="flex justify-between items-center mb-5 text-sm">
                    <span className="text-tosssub font-medium">총 예상 금액</span>
                    <span className="font-extrabold text-lg text-tosstext">{estimatedTotal.toLocaleString()}원</span>
                  </div>

                  <button 
                    onClick={handleOrder}
                    className={`w-full py-4 rounded-2xl text-white font-bold text-lg shadow-md transition active:scale-[0.98] ${orderType === 'BUY' ? 'bg-tossred hover:bg-red-600' : 'bg-tossblue hover:bg-blue-600'}`}
                  >
                    {orderType === 'BUY' ? '매수하기' : '매도하기'}
                  </button>
                </div>
              </div>

            </div>

            {/* 학교 검색 모달 */}
            {showLoginModal && (
              <div className="fixed inset-0 bg-black/40 backdrop-blur-sm z-50 flex items-center justify-center p-4">
                <div className="bg-white w-full max-w-md rounded-3xl p-6 shadow-2xl animate-fade-in-down">
                  <div className="flex justify-between items-center mb-2">
                    <h3 className="text-xl font-bold text-tosstext">학교 및 참가자 설정</h3>
                    <button onClick={() => setShowLoginModal(false)} className="text-slate-400 hover:text-slate-600 p-1">
                      ✕
                    </button>
                  </div>
                  <p className="text-xs text-tosssub mb-5">학교를 검색하여 선택하면 학교 대항전 랭킹에 소속됩니다.</p>
                  
                  <form onSubmit={handleLogin} className="space-y-4">
                    <div>
                      <label className="text-xs font-bold text-slate-600 mb-1.5 block">참가자 이름 (닉네임)</label>
                      <input 
                        type="text" 
                        value={inputName} 
                        onChange={(e) => setInputName(e.target.value)}
                        placeholder="예: 홍길동"
                        className="w-full bg-slate-50 border border-slate-200 rounded-2xl px-4 py-3 text-sm font-semibold outline-none focus:border-tossblue focus:bg-white transition"
                      />
                    </div>

                    <div>
                      <label className="text-xs font-bold text-slate-600 mb-1.5 flex justify-between items-center">
                        <span>소속 학교 검색 (부산 전역 및 전국)</span>
                        {inputSchool && (
                          <span className="text-[11px] text-tossblue font-normal">
                            선택됨: {inputSchool}
                          </span>
                        )}
                      </label>
                      
                      <input 
                        type="text" 
                        value={inputSchool} 
                        onChange={(e) => setInputSchool(e.target.value)}
                        placeholder="학교 이름을 검색하세요 (예: 센텀, 해운대, 부산대, 사직)"
                        className="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-2xl text-sm font-semibold outline-none focus:border-tossblue focus:bg-white transition"
                      />

                      <div className="mt-2 max-h-48 overflow-y-auto border border-slate-100 rounded-2xl bg-slate-50/50 divide-y divide-slate-100">
                        {schoolSearchResults.length > 0 ? (
                          schoolSearchResults.map((school) => (
                            <div 
                              key={school.name}
                              onClick={() => setInputSchool(school.name)}
                              className={`p-3 flex items-center justify-between cursor-pointer hover:bg-blue-50/60 transition ${inputSchool === school.name ? 'bg-blue-50' : ''}`}
                            >
                              <div className="flex items-center gap-2.5">
                                <span className="text-lg">🏫</span>
                                <div>
                                  <div className="text-sm font-bold text-tosstext">{school.name}</div>
                                  <div className="text-[11px] text-tosssub">
                                    📍 {school.region} • {school.type}
                                  </div>
                                </div>
                              </div>
                              {inputSchool === school.name && (
                                <span className="text-tossblue font-bold">✓</span>
                              )}
                            </div>
                          ))
                        ) : (
                          <div className="p-3 text-xs text-slate-500 text-center">
                            "{inputSchool}"(으)로 직접 입력하여 참가하기
                          </div>
                        )}
                      </div>
                    </div>
                    
                    <div className="flex gap-2 pt-3">
                      <button 
                        type="button" 
                        onClick={() => setShowLoginModal(false)}
                        className="flex-1 py-3 text-slate-500 font-bold text-sm bg-slate-100 rounded-xl hover:bg-slate-200 transition"
                      >
                        닫기
                      </button>
                      <button 
                        type="submit" 
                        className="flex-1 py-3 text-white font-bold text-sm bg-tossblue rounded-xl hover:bg-blue-600 transition shadow-md"
                      >
                        설정 완료
                      </button>
                    </div>
                  </form>
                </div>
              </div>
            )}

          </div>
        );
      }

      ReactDOM.createRoot(document.getElementById('root')).render(<App />);
    </script>
  </body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    return HTMLResponse(content=HTML_TEMPLATE, status_code=200)
