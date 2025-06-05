import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import json
import urllib.parse
from gspread_formatting import format_cell_range, CellFormat, TextFormat, set_column_width, Color, Padding
from tenacity import retry, stop_after_attempt, wait_exponential
import os
SHEET_URL = os.environ.get("SHEET_URL")

SHEET_5GIAY_URL = SHEET_URL  # Dùng cùng 1 link Google Sheet
today_str = datetime.now().strftime("%d-%m-%Y")
compare_sheet_name = f"Check-Gia-MKCOM-{today_str}"

def get_all_barebone_links():
    base_url = "https://minhkhoicomputer.com/product-category/danh-muc-san-pham/barabone/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    product_links = dict()
    page = 1
    while True:
        url = base_url if page == 1 else f"{base_url}page/{page}/"
        print(f"Đang lấy link từ: {url}")
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            print("  -> LỖI: Không truy cập được trang danh mục này!")
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all('a', href=True):
            if '/product/' in a['href']:
                full_link = a['href'] if a['href'].startswith('http') else f"https://minhkhoicomputer.com{a['href']}"
                if full_link not in product_links:
                    product_links[full_link] = page
        next_btn = soup.find('a', class_='next page-numbers')
        if not next_btn:
            break
        page += 1
        time.sleep(1)
    # Trả về list các tuple (link, page)
    return [(link, product_links[link]) for link in product_links]

def get_barebone_info(url, page):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"LỖI: Không truy cập được sản phẩm {url}")
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    # Lấy bảng thông tin
    table = soup.find('table', class_=['notcauhinh', 'cauhinh'])
    results = []
    if table:
        rows = table.find_all('tr')
        for row in rows[1:]:  # bỏ header
            tds = row.find_all('td')
            if len(tds) > 1:
                name = tds[0].text.strip()
                # Xóa thông tin trong dấu ngoặc đơn
                name = re.sub(r'\([^)]*\)', '', name).strip()
                # Thêm "[Đang Ẩn]" nếu tr có thuộc tính hidden
                if row.has_attr('hidden'):
                    name = f"{name} [Đang Ẩn]"
                # Chỉ lấy dòng có chữ 'barebone' (không phân biệt hoa thường)
                if 'barebone' not in name.lower():
                    continue
                price_text = tds[1].text
                strong = tds[1].find('strong')
                if strong:
                    price_text = strong.text
                price = price_text.strip().replace('.', '').replace(',', '').replace('VND', '').replace('đ', '')
                try:
                    price = int(price)
                except:
                    price = None
                print(f"Tên: {name} | Giá: {price} | Link: {url} | Trang: {page}")
                post_title = ""
                h2 = soup.find('h2', class_='product-name')
                if h2:
                    post_title = h2.text.strip()
                results.append({
                    "Tên Post": post_title,
                    "Tên sản phẩm": name,
                    "Giá bán (VNĐ)": price,
                    "Link": url,
                    "Trang": page
                })
    else:
        print(f"Không tìm thấy bảng thông tin tại {url}")
    return results

def upload_to_gsheets(df, sheet_url, worksheet_name="Sheet1"):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    sh = client.open_by_url(sheet_url)
    try:
        worksheet = sh.worksheet(worksheet_name)
        worksheet.clear()
    except:
        worksheet = sh.add_worksheet(title=worksheet_name, rows="300", cols="5")
    # Thay thế NaN bằng None hoặc chuỗi rỗng
    df = df.fillna("")  # Hoặc df = df.fillna(None)
    # Ghi header + data
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"Đã upload dữ liệu lên Google Sheets: {worksheet_name}")

    # Sau khi đã upload dữ liệu lên sheet và có biến worksheet
    header_format = CellFormat(
        backgroundColor=Color(0.2, 0.6, 0.86),  # Xanh dương nhạt, đổi theo ý bạn
        textFormat=TextFormat(bold=True, fontFamily='Arial', fontSize=11, foregroundColor=Color(1,1,1)),  # Chữ trắng, in đậm
        padding=Padding(left=5, top=5, right=5, bottom=5)
    )
    format_cell_range(worksheet, 'A1:G1', header_format)

    number_format = CellFormat(
        numberFormat={'type': 'NUMBER', 'pattern': '#,##0'},
        horizontalAlignment='RIGHT'
    )

    format_cell_range(worksheet, 'C1:E1000', number_format)

    padding_format = CellFormat(
        padding=Padding(left=5, top=5, right=5, bottom=5)
    )
    format_cell_range(worksheet, 'A2:F1000', padding_format)

    set_column_width(worksheet, 'A', 300)
    set_column_width(worksheet, 'B', 400)
    set_column_width(worksheet, 'C', 111)
    set_column_width(worksheet, 'D', 111)
    set_column_width(worksheet, 'E', 91)
    set_column_width(worksheet, 'F', 200)
    set_column_width(worksheet, 'G', 200)

    left_middle_format = CellFormat(
        horizontalAlignment='LEFT',
        verticalAlignment='MIDDLE'
    )
    format_cell_range(worksheet, 'A1:A1000', left_middle_format)

    left_format = CellFormat(
        horizontalAlignment='LEFT',
    )
    format_cell_range(worksheet, 'B1:B1000', left_format)
    format_cell_range(worksheet, 'F1:G1000', left_format)

    middle_format = CellFormat(
        verticalAlignment='MIDDLE'
    )
    format_cell_range(worksheet, 'A1:G1000', middle_format)

    worksheet.freeze(rows=1)
    # worksheet.freeze(rows=1, cols=1)

    return worksheet

def merge_link_cells(sheet_url, worksheet_name="Sheet1", link_col=3):
    # link_col: cột Link, mặc định là cột thứ 3 (A=1, B=2, C=3, ...)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    sh = client.open_by_url(sheet_url)
    worksheet = sh.worksheet(worksheet_name)
    data = worksheet.get_all_values()
    last_link = None
    start_row = None
    for i, row in enumerate(data[1:], start=2):  # Bỏ header, bắt đầu từ dòng 2
        link = row[link_col-1]
        if link == last_link:
            # Đang trong chuỗi giống nhau
            continue
        else:
            # Nếu có chuỗi trước đó dài hơn 1, thì merge
            if start_row and i-1 > start_row:
                worksheet.merge_cells(
                    start_row, link_col, i-1, link_col
                )
            last_link = link
            start_row = i
    # Merge chuỗi cuối cùng nếu cần
    if start_row and len(data) > start_row:
        worksheet.merge_cells(
            start_row, link_col, len(data), link_col
        )
    print("Đã merge các ô link giống nhau.")

def has_factor(part):
    # Kiểm tra part có chứa 1 trong các factor không
    return bool(re.search(r'\b(sff|mt|dt|mini|tiny)\b', part, re.IGNORECASE))

def get_all_5giay_prices(sheet_url, sheet_date):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    sh = client.open_by_url(sheet_url)
    worksheet = sh.worksheet(sheet_date)
    data = worksheet.get_all_records()
    
    # In ra tên các cột để kiểm tra
    print("Các cột trong sheet:", list(data[0].keys()) if data else "Không có dữ liệu")
    
    # Thử lấy giá từ các cột có thể chứa giá
    price_dict = {}
    for row in data:
        name_key = row.get("Tên SP đã sửa") or row.get("Tên sản phẩm")
        if not name_key:
            continue
        name_key = name_key.lower().strip()
        price = None
        if "Giá bán VC" in row:
            price = row["Giá bán VC"]
        elif "Giá bán (VNĐ)" in row:
            price = row["Giá bán (VNĐ)"]
        elif "Giá" in row:
            price = row["Giá"]
        # Lưu cả chuỗi gốc làm key
        price_dict[name_key] = price

        # Split theo / nếu tất cả các phần đều có factor
        parts = [p.strip() for p in name_key.split('/')]
        if len(parts) > 1 and all(has_factor(p) for p in parts):
            for part in parts:
                price_dict[part] = price
    
    return price_dict

def add_5giay_price_and_diff(df):
    # Đổi tên các cột trước
    df = df.rename(columns={
        "Tên sản phẩm": "Tên SP MKCOM",
        "Giá bán (VNĐ)": "Giá MKCOM"
    })

    prices_5giay = []
    diffs = []
    all_5giay_prices = get_all_5giay_prices(SHEET_5GIAY_URL, today_str)
    
    # In ra số lượng sản phẩm tìm thấy giá
    found_prices = sum(1 for price in all_5giay_prices.values() if price is not None)
    print(f"Tìm thấy giá cho {found_prices}/{len(all_5giay_prices)} sản phẩm")
    
    for idx, row in df.iterrows():
        product_name = row["Tên SP MKCOM"].lower().strip()
        price_5giay = get_price_from_5giay(product_name, all_5giay_prices)
        prices_5giay.append(price_5giay)
        
        # Tính chênh lệch chỉ khi có giá ở cả 2 bên
        if price_5giay != "-" and row["Giá MKCOM"] is not None:
            try:
                diff = row["Giá MKCOM"] - int(price_5giay)
                # Nếu chênh lệch = 0 thì gán "-"
                if diff == 0:
                    diff = "-"
            except:
                diff = ""
        else:
            diff = ""
        diffs.append(diff)
    
    # Xóa cột Trang
    df = df.drop(columns=["Trang"])
    
    # Thêm cột Giá 5giay và Chênh lệch
    cols = list(df.columns)
    price_col_idx = cols.index("Giá MKCOM")
    df.insert(price_col_idx + 1, "Giá 5giay", prices_5giay)
    df.insert(price_col_idx + 2, "Chênh lệch", diffs)
    
    # Thêm link và các cột khác
    df["Link"] = df.apply(lambda row: make_text_fragment_link(row["Link"], row["Tên SP MKCOM"]), axis=1)
    
    cols = df.columns.tolist()
    cols = ['Tên Post'] + [col for col in cols if col != 'Tên Post']
    df = df[cols]
    
    return df

def chuan_hoa_ten(name):
    return re.sub(r'\s*([\\/|])\s*', r'\1', name.strip().lower())

def extract_model_part(name):
    # Chuyển về chữ thường để dễ xử lý
    name = name.lower()
    # Loại bỏ tiền tố "barebone" và hãng nếu có
    # Ví dụ: "barebone dell 400g1 sff" -> "400g1 sff"
    match = re.search(r'barebone\s+(dell|hp|lenovo)?\s*(.*)', name)
    if match:
        return match.group(2).strip()
    return name.strip()

def get_price_from_5giay(name, price_dict):
    name = name.lower().strip()
    # So khớp trực tiếp với tên SP đã sửa
    if name in price_dict:
        return price_dict[name]
    return "-"  # Trả về "-" nếu không tìm thấy tên giống nhau

def make_text_fragment_link(base_url, product_name):
    encoded_text = urllib.parse.quote(product_name)
    return f"{base_url}#:~:text={encoded_text}"

def add_arrow_to_price(row):
    diff = row["Chênh lệch"]
    price = row["Giá MKCOM"]
    if diff == "" or price == "":
        return price
    try:
        diff_val = int(diff)
        if diff_val == 0:
            return f"{price:,}"
        elif diff_val < 0:
            arrow = "🔺"  # Mũi tên lên
        else:
            arrow = "🔻"  # Mũi tên xuống
        return f"{arrow} {price:,}"
    except:
        return price

def clear_duplicate_post_title(df):
    df = df.copy()
    df['Tên Post'] = df['Tên Post'].where(df['Tên Post'].ne(df['Tên Post'].shift()))
    return df

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_post_id_from_shortlink(product_url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(product_url, headers=headers, timeout=5)  # Giảm timeout xuống 5s
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        shortlink_tag = soup.find("link", rel="shortlink")
        if shortlink_tag and "href" in shortlink_tag.attrs:
            match = re.search(r"p=(\d+)", shortlink_tag["href"])
            if match:
                return match.group(1)
        return ""
    except Exception as e:
        print(f"Lỗi khi lấy post ID từ {product_url}: {str(e)}")
        return ""

def add_post_id_column(df):
    # Giả sử cột "Link" chứa link sản phẩm
    ids = []
    for url in df["Link"]:
        post_id = get_post_id_from_shortlink(url)
        ids.append(post_id)
    # Thêm cột "ID" sau cột "Chênh lệch"
    insert_idx = df.columns.get_loc("Chênh lệch") + 1
    df.insert(insert_idx, "ID", ids)
    return df

def add_edit_price_column(df):
    # Lấy ID từ cột "ID" và tạo link sửa (KHÔNG thêm text fragment)
    edit_links = []
    for post_id in df["ID"]:
        if post_id:
            edit_link = f"https://minhkhoicomputer.com/wp-admin/post.php?post={post_id}&action=edit"
        else:
            edit_link = ""
        edit_links.append(edit_link)
    # Thêm cột "Sửa Giá" sau cột "Link"
    insert_idx = df.columns.get_loc("Link") + 1
    df.insert(insert_idx, "Sửa Giá", edit_links)
    # Xóa cột "ID"
    df = df.drop(columns=["ID"])
    return df

if __name__ == "__main__":
    all_links = get_all_barebone_links()
    print(f"Tổng số link sản phẩm: {len(all_links)}")
    all_products = []
    for idx, (link, page) in enumerate(all_links, 1):
        print(f"({idx}/{len(all_links)}) Đang lấy: {link} (Trang {page})")
        infos = get_barebone_info(link, page)
        all_products.extend(infos)
        time.sleep(1)
    # Chuyển sang DataFrame
    df = pd.DataFrame(all_products)
    # Upload lên Google Sheets
    df = add_5giay_price_and_diff(df)
    df["Giá MKCOM"] = df.apply(add_arrow_to_price, axis=1)
    df = clear_duplicate_post_title(df)
    df = df.loc[df['Tên Post'].ne(df['Tên Post'].shift())].reset_index(drop=True)
    total = df['Tên Post'].replace('', pd.NA).dropna().shape[0]
    df = df.rename(columns={"Tên Post": f"Tên Post [{total}]"})
    # Thêm post ID và link sửa
    df = add_post_id_column(df)
    df = add_edit_price_column(df)
    upload_to_gsheets(df, SHEET_URL, worksheet_name=compare_sheet_name)
