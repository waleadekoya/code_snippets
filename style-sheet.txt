import pandas as pd

class StyledExcelWriter:
    
    def __init__(self, filename):
        self.filename = filename

    def write(self, df, sheet_name='Sheet1'):
        with pd.ExcelWriter(self.filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            worksheet = writer.sheets[sheet_name]
            workbook = writer.book
            
            # Format definitions
            header_format = workbook.add_format({
                'bold': True,
                'font_name': 'Arial',
                'bg_color': 'red',
                'border': 1,
                'font_color': 'white'
            })
            
            text_format = workbook.add_format({
                'font_name': 'Arial'
            })
            
            number_format = workbook.add_format({
                'font_name': 'Arial',
                'num_format': '#,##0',
                'align': 'right'
            })
            
            # Setting column width and format
            for idx, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).apply(len).max(), len(col))
                worksheet.set_column(idx, idx, max_len, text_format if df[col].dtype == 'object' else number_format)
                worksheet.write(0, idx, col, header_format)
            
            # Freeze top row
            worksheet.freeze_panes(1, 0)
            
            # Zoom to 90%
            worksheet.set_zoom(90)
