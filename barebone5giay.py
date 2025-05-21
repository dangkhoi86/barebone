import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import format_cell_range, CellFormat, TextFormat, set_column_width, Color, Padding
import unicodedata
import time

def format_price(raw_price):
    if not raw_price:
        return 0
    raw_price = raw_price.lower().replace(' ', '')
    
    # Nếu không có đơn vị k hoặc tr, mặc định là tr
    if not ('tr' in raw_price or 'k' in raw_price):
        # Chuyển 3.10 thành 3.1tr
        num = float(raw_price.replace(',', '.'))
        raw_price = f"{num}tr"
    
    # Xử lý cả k và tr
    if 'tr' in raw_price or 'k' in raw_price:
        num = raw_price.replace('tr', '').replace('k', '').replace(',', '.')
        try:
            # Nếu có dấu phẩy hoặc dấu chấm trong số gốc thì nhân 1,000,000
            if ',' in raw_price or '.' in raw_price:
                price = float(num) * 1_000_000
            # Nếu không có dấu phẩy/dấu chấm
            else:
                # Nếu là tr thì nhân 1,000,000
                if 'tr' in raw_price:
                    price = float(num) * 1_000_000
                # Nếu là k thì nhân 1,000
                else:
                    price = float(num) * 1_000
            return int(price)
        except:
            return 0
    
    return 0

def format_price_str(price):
    if price:
        return int(price)  # Trả về giá trị số nguyên, không chuyển sang chuỗi
    return 0

def get_form_factor(name, so_tan=''):
    name_lower = name.lower()
    # Ưu tiên SFF, TINY, MT, DT
    if 'sff' in name_lower:
        return 'SFF'
    if 'tiny' in name_lower:
        return 'TINY'
    if 'mini' in name_lower:
        return 'MINI'
    if 'mt' in name_lower:
        return 'MT'
    if 'dt' in name_lower:
        return 'DT'
    # Nếu có 2 tản thì chắc chắn là WORK
    if so_tan == '2':
        return 'WORK'
    # Nhận diện các model workstation phổ biến
    workstation_patterns = [
        r'\bs\d{2,4}\b',         # S30, S20, S40 (Lenovo)
        r'\bp\d{3,4}c?\b',       # P510, P520, P520c, P720, P920 (Lenovo)
        r'\bw\d{3,4}\b',         # W530, W540 (Lenovo)
        r'\bt\d{3,4}\b',         # T5820, T7820 (Dell)
        r'\bz\d{1,4}\b',         # Z420, Z820 (HP)
        r'workstation',
        r'precision',
    ]
    for pat in workstation_patterns:
        if re.search(pat, name_lower):
            return 'WORK'
    return ''

def is_workstation(name):
    name_lower = name.lower()
    return (
        'workstation' in name_lower or
        'precision' in name_lower or
        re.search(r'\\bz\\d{3}\\b', name_lower) or  # HP Zxxx
        re.search(r'\\bt\\d{3,4}\\b', name_lower)   # Dell Txxx/Txxxx
    )

def extract_model(name):
    # Chỉ lấy phần trước dấu +
    name = name.split('+')[0].strip()
    # Loại bỏ phần trong ngoặc ()
    name = re.sub(r'\(.*?\)', '', name)
    # Tách theo / hoặc - nếu có, nhưng chỉ lấy phần model, không lấy phần là chip
    parts = re.split(r'\s*[/-]\s*', name)
    models = []
    for part in parts:
        # Ưu tiên lấy các mẫu model phổ biến, không lấy tên chip, không lấy Optiplex/Precision...
        patterns = [
            r'\bTP\d{2,4}[a-zA-Z]?\b',   # HP Pavilion TP01, ...
            r'\bS\d{2,4}[a-zA-Z]?\b',    # Lenovo S30, S20, ...
            r'\bE\d{2,4}[a-zA-Z]?\b',    # E92p, E93, E73, E32, ...
            r'\bM\d{2,4}[a-zA-Z]?\b',    # M73, M83, M710, M720, ...
            r'\bP\d{3,4}[a-zA-Z]?\b',    # P320, P330, ...
            r'\bV\d{3,4}[a-zA-Z]?\b',    # V520, ...
            r'\bPRECISION\s+\d{3,4}\b',    # PRECISION 3630, ...
            r'\bTHINKCENTRE\s+\w+\s*G\d{1,2}\b',  # THINKCENTRE M720 G1, ...
            r'\bP\d{3,4}[A-Z]?\b',         # P520C, P520, P310, P340, ...
            r'\bE\d{2,4}[A-Z]?\b',         # E93, E73, E32, ...
            r'\bZ\d{1,4}\s*G\d{1,2}\b',    # Z4 G4, Z240 G2, ...
            r'\bZ\d{1,4}\b',               # Z420, Z820, ...
            r'\bT\d{3,4}\b',               # T1700, T3420, T7820, ...
            r'\b\d{3,4}\s*G\d{1,2}\b',     # 600 G1, 800 G2, ...
            r'\bG\d{1,2}\b',               # G1, G2, ...
            r'\bXE2\b',                    # XE2
            r'\b\d{3,4}\b',                # 3020, 3050, 7050, 600, 800, ...
        ]
        for pat in patterns:
            m = re.search(pat, part, re.IGNORECASE)
            if m:
                val = m.group(0).upper().strip()
                # Không lấy model là số đơn lẻ nếu đã có model dạng chữ+số
                if re.match(r'^\d{3,4}$', val) and any(re.match(r'^[A-Z]+\d', v) for v in models):
                    continue
                models.append(val)
                break
    # Loại bỏ trùng lặp
    models = list(dict.fromkeys(models))
    return ' | '.join(models)

def extract_psu(line):
    # Chỉ lấy các trường hợp có dạng số + w)
    psu_matches = re.findall(r'(\d{3,4}w\))', line, re.IGNORECASE)
    if psu_matches:
        # Loại bỏ dấu ) khi trả về
        return ' / '.join([x.replace(')', '').upper() for x in psu_matches])
    return ''

def chuan_hoa_ten_sp_da_sua(name):
    # Xoá toàn bộ thông tin trong dấu ngoặc đơn (kể cả dấu ngoặc)
    name = re.sub(r'\([^)]*\)', '', name)
    # Xoá mọi dấu ngoặc đơn dư thừa còn sót lại
    name = name.replace('(', '').replace(')', '')
    # Đổi dấu - thành / và xoá khoảng trắng dư thừa quanh dấu này
    name = re.sub(r'\s-\s', '/', name)
    # Chuẩn hóa lại khoảng trắng quanh /
    name = re.sub(r'\s*/\s*', '/', name)
    # Chỉ xóa khoảng trắng trước Gx khi sau nó có form factor
    name = re.sub(r' (\b[gG][1-8]\b)\s+(sff|mt|dt|mini|tiny)\b', r'\1 \2', name, flags=re.IGNORECASE)
    # Thêm khoảng trắng trước V1, V2, ... nếu liền với từ trước
    name = re.sub(r'([a-zA-Z0-9])([vV][1-9]\b)', r'\1 \2', name)
    # Tách Barebone dính liền với tên hãng
    name = re.sub(r'^(Barebone)(?=[A-Z])', r'\1 ', name)
    # Xóa từ Prodesk (không phân biệt hoa thường)
    name = re.sub(r'\bProdesk\b', '', name, flags=re.IGNORECASE)
    # Loại bỏ mọi khoảng trắng dư thừa sau cùng
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def add_factor_to_model_pairs(name):
    # Tìm các trường hợp có dạng model/model + factor
    # Ví dụ: 3046/7040 Mt -> 3046 Mt/7040 Mt
    if '/' in name:
        parts = [p.strip() for p in name.split('/')]
        if len(parts) == 2:
            # Kiểm tra phần cuối có factor (ví dụ: "7040 Mt")
            m = re.match(r'([a-zA-Z0-9 ]+)\s+([a-zA-Z]+)$', parts[1])
            if m:
                model2, factor = m.group(1).strip(), m.group(2).strip()
                # Kiểm tra phần đầu đã có factor chưa
                m1 = re.match(r'([a-zA-Z0-9 ]+)\s+([a-zA-Z]+)$', parts[0])
                if not m1:
                    # Nếu phần đầu chưa có factor, mới thêm
                    parts[0] = f"{parts[0]} {factor}"
                    return '/'.join(parts)
    return name

def chuan_hoa_cpu(cpu_str):
    # Silver
    cpu_str = re.sub(r'(41\d{2})\s*[xX]\s*2', r'2 Xeon Silver 4110', cpu_str)
    # Gold
    cpu_str = re.sub(r'(51\d{2})\s*[xX]\s*2', r'2 Xeon Gold', cpu_str)
    # E5
    cpu_str = re.sub(r'(26\d{2})\s*[xX]\s*2', r'2 Xeon E5', cpu_str)
    # E7
    cpu_str = re.sub(r'(88\d{2})\s*[xX]\s*2', r'2 Xeon E7', cpu_str)
    # Nếu không nhận diện được thì để mặc định
    cpu_str = re.sub(r'([0-9]{4,5})\s*[xX]\s*2', r'2 Xeon', cpu_str)
    return cpu_str

def crawl_5giay():
    url = "https://www.5giay.vn/threads/vi-tinh-bao-nhu-case-pc-may-bo-dell-hp-lenovo-gia-re.34567/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    products = []
    for block in soup.find_all('blockquote'):
        block_text = block.get_text(separator='\n')
        for line in block_text.splitlines():
            if 'barebone' in line.lower():
                line_norm = unicodedata.normalize('NFC', line)
                match = re.search(r'^(.*?)\s*[,/\-]*\s*giá\s*([\d\.,kKtrTR]+)', line_norm, re.IGNORECASE)
                
                if match:
                    name = match.group(1).strip()
                    name = BeautifulSoup(name, "html.parser").get_text()
                    
                    barebone_pos = name.lower().find('barebone')
                    if barebone_pos > 0:
                        name = name[barebone_pos:].strip()
                    
                    name = re.sub(r'\s+', ' ', name)
                    raw_price = match.group(2)
                    price = format_price(raw_price) if raw_price else 0
                    
                    if not price:
                        continue
                            
                    # Debug các thông tin khác
                    hang = "Dell" if "Dell" in name else ("HP" if "Hp" in name or "HP" in name else "Lenovo")
                    model = extract_model(name)
                    model = model.replace("PRECISION ", "").replace("PRECISION", "")
                    
                    # Xác định số tản CPU
                    so_tan = ""
                    if (
                        re.search(r"2\s*(tản|fan|tản nhiệt)", line, re.IGNORECASE)
                        or re.search(r"x\s*2", name, re.IGNORECASE)
                        or re.search(r"\+\s*2\s*xeon", name, re.IGNORECASE)
                    ):
                        so_tan = "2"

                    form = get_form_factor(name, so_tan)
                    cpu = ""
                    cpu_match = re.search(r"\+\s*([A-Za-z0-9\s]+)", name)
                    if cpu_match:
                        cpu = cpu_match.group(1).strip()
                        cpu = chuan_hoa_cpu(cpu)

                    # PSU
                    psu = extract_psu(line)

                    # Tính +VC và Giá bán VC
                    vc_plus = 0
                    if form == 'WORK':
                        if so_tan == '2':
                            vc_plus = 500000
                        else:
                            vc_plus = 400000
                    elif form == 'SFF':
                        vc_plus = 300000
                    elif form == 'MT':
                        vc_plus = 400000
                    elif form == 'TINY':
                        vc_plus = 100000
                    elif form == 'MINI':
                        vc_plus = 100000
                    price_vc = price + vc_plus

                    original_name = name
                    name_sua = chuan_hoa_ten_sp_da_sua(original_name)
                    name_sua = add_factor_to_model_pairs(name_sua)
                    name_sua = chuan_hoa_cpu(name_sua)
                    
                    products.append({
                        "Tên SP Gốc": clean_barebone_prefix(line),
                        "Tên SP đã sửa": name_sua,
                        "Hãng": hang,
                        "Model/Series": model,
                        "Form Factor": form,
                        "Số tản CPU": ('2 tản' if so_tan == '2' else '1 tản'),
                        "PSU": psu if psu else "-",
                        "Giá bán (VNĐ)": format_price_str(price),
                        "+ VC": format_price_str(vc_plus) if vc_plus else '',
                        "Giá bán VC": format_price_str(price_vc),
                        "CPU đi kèm": cpu if cpu else "-"
                    })
    return products

def write_to_sheet(products):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    
    # Tạo tên sheet theo ngày crawl
    today = datetime.now().strftime("%d-%m-%Y")
    sh = client.open_by_url("https://docs.google.com/spreadsheets/d/1N-aHLsVYKt9H_7xc4thFb1Ey6h2Mj7F93Ett_uEEMZQ/edit#gid=0")
    
    # Kiểm tra sheet đã tồn tại chưa
    try:
        worksheet = sh.worksheet(today)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=today, rows="300", cols="11")
    
    # Ghi header
    worksheet.append_row([
        "Tên SP Gốc", "Tên SP đã sửa", "Hãng", "Model/Series", "Form Factor", "Số tản CPU", "PSU", "Giá bán (VNĐ)", "+ VC", "Giá bán VC", "CPU đi kèm"
    ])

    # Ghi toàn bộ dữ liệu một lần
    rows = []
    for p in products:
        rows.append([
            p["Tên SP Gốc"],
            p["Tên SP đã sửa"],
            p["Hãng"],
            p["Model/Series"],
            p["Form Factor"],
            p["Số tản CPU"],
            p["PSU"],
            p["Giá bán (VNĐ)"],
            p["+ VC"],
            p["Giá bán VC"],
            p["CPU đi kèm"]
        ])
    if rows:
        worksheet.append_rows(rows, value_input_option='USER_ENTERED')

    print(f"Số sản phẩm sẽ ghi lên sheet: {len(products)}")

    for p in products[:5]:
        print("Sản phẩm mẫu:", p)

    print("Số dòng sẽ ghi:", len(rows))

    # Format header
    header_format = CellFormat(
        backgroundColor=Color(0.2, 0.6, 0.86),  # Xanh dương nhạt
        textFormat=TextFormat(bold=True, fontFamily='Arial', fontSize=11, foregroundColor=Color(1,1,1)),  # Chữ trắng, in đậm
        padding=Padding(left=5, top=5, right=5, bottom=5)
    )
    format_cell_range(worksheet, 'A1:K1', header_format)

    number_format = CellFormat(
        numberFormat={'type': 'NUMBER', 'pattern': '#,##0'},
        horizontalAlignment='RIGHT'
    )

    format_cell_range(worksheet, 'H1:H1000', number_format)
    format_cell_range(worksheet, 'I1:I1000', number_format)
    format_cell_range(worksheet, 'J1:J1000', number_format)

    center_format = CellFormat(
        horizontalAlignment='CENTER',
        verticalAlignment='MIDDLE'
    )
    format_cell_range(worksheet, 'C1:C1000', center_format)
    format_cell_range(worksheet, 'D1:D1000', center_format)
    format_cell_range(worksheet, 'E1:E1000', center_format)
    format_cell_range(worksheet, 'F1:F1000', center_format)
    format_cell_range(worksheet, 'G1:G1000', center_format)
    format_cell_range(worksheet, 'K1:K1000', center_format)

    padding_format = CellFormat(
        padding=Padding(left=5, top=5, right=5, bottom=5)
    )
    format_cell_range(worksheet, 'A2:K1000', padding_format)

    set_column_width(worksheet, 'A', 550)
    set_column_width(worksheet, 'B', 380)
    set_column_width(worksheet, 'C', 80)
    set_column_width(worksheet, 'D', 120)
    set_column_width(worksheet, 'E', 100)
    set_column_width(worksheet, 'F', 100)
    set_column_width(worksheet, 'G', 50)
    set_column_width(worksheet, 'H', 110)
    set_column_width(worksheet, 'I', 80)
    set_column_width(worksheet, 'J', 100)
    set_column_width(worksheet, 'K', 130)

    worksheet.freeze(rows=1)

def remove_duplicates(products):
    # Sử dụng set để loại bỏ các sản phẩm trùng lặp dựa trên tên sản phẩm
    seen = set()
    unique_products = []
    for p in products:
        if p["Tên SP Gốc"] not in seen:
            seen.add(p["Tên SP Gốc"])
            unique_products.append(p)
    return unique_products

def clean_barebone_prefix(line):
    # Xóa dấu '-' và khoảng trắng ở đầu trước từ Barebone
    return re.sub(r'^\s*-\s*', '', line).lstrip()

if __name__ == "__main__":
    try:
        print("Bắt đầu crawl dữ liệu...")
        products = crawl_5giay()
        print(f"Đã crawl được {len(products)} sản phẩm.")
        # Loại bỏ các sản phẩm trùng lặp
        products = remove_duplicates(products)
        print(f"Sau khi loại bỏ trùng lặp, còn {len(products)} sản phẩm.")
        for p in products:
            print(p)
        write_to_sheet(products)
        print("Đã ghi dữ liệu lên Google Sheets.")
    except Exception as e:
        print("Lỗi:", e)