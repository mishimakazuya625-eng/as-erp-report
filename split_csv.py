import csv

input_file = r'c:\Project\ASERP\bom_template.csv'
output_dir = r'c:\Project\ASERP'
rows_per_file = 500

with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)
    # Fix header by removing spaces
    header = [col.strip() for col in header]
    
    file_num = 1
    rows = []
    row_count = 0
    
    for row in reader:
        rows.append(row)
        row_count += 1
        
        if row_count >= rows_per_file:
            output_file = f'{output_dir}\\bom_template_part{file_num}.csv'
            with open(output_file, 'w', newline='', encoding='utf-8') as out:
                writer = csv.writer(out)
                writer.writerow(header)
                writer.writerows(rows)
            print(f'Created {output_file} with {row_count} rows')
            
            file_num += 1
            rows = []
            row_count = 0
    
    # Write remaining rows
    if rows:
        output_file = f'{output_dir}\\bom_template_part{file_num}.csv'
        with open(output_file, 'w', newline='', encoding='utf-8') as out:
            writer = csv.writer(out)
            writer.writerow(header)
            writer.writerows(rows)
        print(f'Created {output_file} with {len(rows)} rows')

print(f'Total files created: {file_num}')
