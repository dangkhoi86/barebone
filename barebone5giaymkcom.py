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
WC_API_URL = os.environ.get("MK_WC_API_URL")
WC_CONSUMER_KEY = os.environ.get("MK_WC_CONSUMER_KEY")
WC_CONSUMER_SECRET = "os.environ.get("MK_WC_CONSUMER_SECRET")
SHEET_5GIAY_URL = SHEET_URL

today_str = datetime.now().strftime("%d-%m-%Y")
compare_sheet_name = f"Check-Gia-MKCOM-{today_str}"

def get_all_barebone_products():
    """Lấy tất cả sản phẩm barebone thông qua WooCommerce API"""
    products = [] # This will accumulate all rows for the DataFrame
    page = 1
    per_page = 100
    
    while True:
        url = f"{WC_API_URL}/products"
        params = {
            'consumer_key': WC_CONSUMER_KEY,
            'consumer_secret': WC_CONSUMER_SECRET,
            'per_page': per_page,
            'page': page,
            'search': 'barebone'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                break
                
            for product_api_data in data: # Renamed to avoid conflict with product variable in inner loop
                post_title = product_api_data.get('name', '')
                product_link = product_api_data.get('permalink', '')
                status = product_api_data.get('status', '')
                modified_by = product_api_data.get('modified_by', '')
                description = product_api_data.get('description', '')
                
                status_icon = "-"
                if status == 'publish':
                    status_icon = '👀'
                elif status == 'pending':
                    status_icon = '⏰'
                elif status == 'private':
                    status_icon = '🔒'
                elif status == 'draft':
                    status_icon = '📝'

                # Biến cờ để kiểm tra xem có sản phẩm barebone nào được trích xuất từ bảng không
                has_extracted_from_table = False
                
                if description:
                    desc_soup = BeautifulSoup(description, "html.parser")
                    # Tìm tất cả các bảng 'notcauhinh' hoặc 'cauhinh'
                    tables = desc_soup.find_all('table', class_=['notcauhinh', 'cauhinh'])
                    
                    if tables:
                        for table in tables:
                            rows_in_table = table.find_all('tr')
                            
                            for row_idx, row in enumerate(rows_in_table):
                                # Bỏ qua hàng tiêu đề nếu có (kiểm tra <th> hoặc nếu là hàng đầu tiên và không có <td>)
                                if row.find('th') or (row_idx == 0 and not row.find_all('td')):
                                    continue 

                                tds = row.find_all('td')
                                if len(tds) > 1: # Đảm bảo có đủ cột cho tên và giá
                                    name_from_table = tds[0].text.strip()
                                    
                                    # Xóa thông tin trong dấu ngoặc đơn
                                    name_from_table = re.sub(r'\([^)]*\)', '', name_from_table).strip()
                                    
                                    # Chỉ lấy dòng có chữ 'barebone' (không phân biệt hoa thường)
                                    if 'barebone' not in name_from_table.lower():
                                        continue 
                                    
                                    # Lấy giá từ bảng (cột thứ 2)
                                    price_from_table = None
                                    price_text = tds[1].text
                                    strong_tag = tds[1].find('strong')
                                    if strong_tag:
                                        price_text = strong_tag.text
                                    price_text = price_text.strip().replace('.', '').replace(',', '').replace('VND', '').replace('đ', '')
                                    try:
                                        price_from_table = int(price_text)
                                    except ValueError:
                                        price_from_table = None

                                    # Xác định "Sản phẩm ẩn" từ hàng hiện tại
                                    current_row_is_hidden = "Đang ẩn" if 'admin-only' in row.get('class', []) else ""

                                    # Áp dụng logic xóa "Precision" và chuẩn hóa khoảng trắng cho tên sản phẩm từ bảng
                                    cleaned_product_name_mkcom = re.sub(r'\bPrecision\b', '', name_from_table, flags=re.IGNORECASE)
                                    cleaned_product_name_mkcom = re.sub(r'\s+', ' ', cleaned_product_name_mkcom).strip()

                                    products.append({
                                        "Tên Post": post_title,
                                        "Tên sản phẩm": cleaned_product_name_mkcom,
                                        "Giá bán (VNĐ)": price_from_table,
                                        "Link": product_link,
                                        "Trang": page,
                                        "Tình trạng": status_icon,
                                        "Người sửa": modified_by,
                                        "Sản phẩm ẩn": current_row_is_hidden
                                    })
                                    has_extracted_from_table = True # Đã trích xuất ít nhất một mục từ bảng
                
                # Nếu không có sản phẩm barebone nào được trích xuất từ bảng, hoặc không tìm thấy bảng nào,
                # thì thêm thông tin sản phẩm chính (từ API)
                if not has_extracted_from_table and 'barebone' in post_title.lower():
                    # Lấy giá sản phẩm chính từ API nếu không có giá cụ thể từ bảng
                    main_product_price = product_api_data.get('price')
                    try:
                        main_product_price = int(float(main_product_price)) if main_product_price else None
                    except:
                        main_product_price = None

                    # Áp dụng làm sạch cho tên bài đăng gốc nếu đó là barebone và không tìm thấy mục nào trong bảng
                    cleaned_post_title_mkcom = re.sub(r'\bPrecision\b', '', post_title, flags=re.IGNORECASE)
                    cleaned_post_title_mkcom = re.sub(r'\s+', ' ', cleaned_post_title_mkcom).strip()

                    products.append({
                        "Tên Post": post_title,
                        "Tên sản phẩm": cleaned_post_title_mkcom, # Tên SP MKCOM sẽ là tên post nếu không có bảng
                        "Giá bán (VNĐ)": main_product_price,
                        "Link": product_link,
                        "Trang": page,
                        "Tình trạng": status_icon,
                        "Người sửa": modified_by,
                        "Sản phẩm ẩn": "" # Không ẩn nếu không phải từ hàng admin-only của bảng
                    })

            page += 1
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi lấy dữ liệu từ API: {str(e)}")
            break
            
    return products

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
        worksheet = sh.add_worksheet(title=worksheet_name, rows="300", cols="10")
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
    format_cell_range(worksheet, 'A1:J1', header_format)

    number_format = CellFormat(
        numberFormat={'type': 'NUMBER', 'pattern': '#,##0'},
        horizontalAlignment='RIGHT'
    )

    format_cell_range(worksheet, 'C1:E1000', number_format)

    padding_format = CellFormat(
        padding=Padding(left=5, top=5, right=5, bottom=5)
    )
    format_cell_range(worksheet, 'A2:J1000', padding_format)

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
    format_cell_range(worksheet, 'J1:J1000', left_format)

    center_format = CellFormat(
        horizontalAlignment='CENTER',
    )
    format_cell_range(worksheet, 'H1:I1000', center_format)

    middle_format = CellFormat(
        verticalAlignment='MIDDLE'
    )
    format_cell_range(worksheet, 'A1:J1000', middle_format)

    worksheet.freeze(rows=1)

    # Điều chỉnh độ rộng cột (Tổng cộng 10 cột)
    set_column_width(worksheet, 'A', 300)
    set_column_width(worksheet, 'B', 400)
    set_column_width(worksheet, 'C', 111)
    set_column_width(worksheet, 'D', 111)
    set_column_width(worksheet, 'E', 91)
    set_column_width(worksheet, 'F', 200)
    set_column_width(worksheet, 'G', 200)
    set_column_width(worksheet, 'H', 90)
    set_column_width(worksheet, 'I', 100)
    set_column_width(worksheet, 'J', 90)

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
    # Lấy tất cả sản phẩm barebone
    all_products = get_all_barebone_products()
    print(f"Tổng số sản phẩm barebone: {len(all_products)}")
    
    # Chuyển sang DataFrame
    df = pd.DataFrame(all_products)
    
    # THÊM ĐOẠN CODE NÀY: Bỏ từ "Precision" và loại bỏ tất cả khoảng trắng dư thừa (đầu, cuối, giữa các từ).
    # Sử dụng re.sub để thay thế từ "Precision" và chuẩn hóa khoảng trắng.
    # regex=True không cần thiết khi dùng .apply(lambda x: re.sub(...))
    df['Tên sản phẩm'] = df['Tên sản phẩm'].apply(lambda x: re.sub(r'\bPrecision\b', '', x, flags=re.IGNORECASE))
    df['Tên sản phẩm'] = df['Tên sản phẩm'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())
    
    # Xử lý và upload lên Google Sheets
    df = add_5giay_price_and_diff(df)
    df["Giá MKCOM"] = df.apply(add_arrow_to_price, axis=1)
    df = clear_duplicate_post_title(df)
    df = df.loc[df['Tên Post'].ne(df['Tên Post'].shift())].reset_index(drop=True)
    total = df['Tên Post'].replace('', pd.NA).dropna().shape[0]
    df = df.rename(columns={"Tên Post": f"Tên Post [{total}]"})
    
    # Thêm post ID và link sửa
    df = add_post_id_column(df)
    df = add_edit_price_column(df)
    
    # Upload lên Google Sheets
    upload_to_gsheets(df, SHEET_URL, worksheet_name=compare_sheet_name)
