import pandas as pd
import numpy as np

# Загрузка данных из Excel-файлов
curators_df = pd.read_excel('C:\\Users\\aibul\\work\\count_risks\\risks\\data\\curators.xlsx')  # Справочник кураторов
projects_df = pd.read_excel('C:\\Users\\aibul\\work\\count_risks\\risks\\data\\NP_FP.xlsx', sheet_name='fp')  # Проекты кураторов
risks_df = pd.read_excel('C:\\Users\\aibul\\work\\count_risks\\risks\\data\\Идентификация.xlsx', skiprows=5)  # Риски

# Объединение таблиц: проекты с кураторами
merged_projects = projects_df.merge(curators_df, left_on='curator', right_on='FIO', how='left')
merged_data = merged_projects.merge(risks_df, left_on='fp_code', right_on='НП (ФП)', how='left')

def process_np_fp(column):
    """Обработка столбца НП (ФП) для получения всех кодов, если есть запятая, иначе возвращаем значение ячейки."""
    
    def get_code(value):
        if pd.isna(value):
            return np.nan  # Если значение NaN, возвращаем NaN

        value = str(value).strip()
        if value:
            # Разделяем по запятой и убираем лишние пробелы
            parts = [part.strip() for part in value.split(',')]
            return parts  # Возвращаем список кодов
        return np.nan

    return column.apply(get_code)

# Обработка столбца НП (ФП)
merged_data['НП (ФП)'] = process_np_fp(merged_data['НП (ФП)'])

def count_unique_risks(data, group_by_col):
    """Подсчет уникальных рисков по каждому куратору без фильтрации."""
    # Разворачиваем списки в отдельные строки
    exploded_data = data.explode('НП (ФП)')
    unique_risks = exploded_data.drop_duplicates(subset=['ID параметра проекта'])
    return unique_risks.groupby(group_by_col)['НП (ФП)'].count().reset_index(name='total_count')

# Подсчет всех параметров без фильтрации
parametrs_counts = count_unique_risks(merged_data, 'FIO')
parametrs_counts.rename(columns={'total_count': 'Количество параметров'}, inplace=True)

# Подсчет показателей
pokazatel_counts = count_unique_risks(merged_data[merged_data['Тип'] == 'Показатели'], 'FIO')
pokazatel_counts.rename(columns={'total_count': 'Количество показателей'}, inplace=True)

# Подсчет мероприятий
meropriatiy_counts = count_unique_risks(merged_data[merged_data['Тип'] == 'Мероприятия'], 'FIO')
meropriatiy_counts.rename(columns={'total_count': 'Количество мероприятий'}, inplace=True)

working_data = merged_data[merged_data['Статус записи'] == 'В работе']

parametrs_working_counts = count_unique_risks(working_data, 'FIO')
parametrs_working_counts.rename(columns={'total_count': 'Количество параметров под риском'}, inplace=True)

pokazatel_working_counts = count_unique_risks(working_data[working_data['Тип'] == 'Показатели'], 'FIO')
pokazatel_working_counts.rename(columns={'total_count': 'Количество показателей под риском'}, inplace=True)

meropriatiy_working_counts = count_unique_risks(working_data[working_data['Тип'] == 'Мероприятия'], 'FIO')
meropriatiy_working_counts.rename(columns={'total_count': 'Количество мероприятий под риском'}, inplace=True)

# Объединение всех результатов в один DataFrame по куратору
final_results = curators_df[['FIO']].merge(parametrs_counts, left_on='FIO', right_on='FIO', how='left')
final_results = final_results.merge(parametrs_working_counts, left_on='FIO', right_on='FIO', how='left')
final_results = final_results.merge(pokazatel_counts, left_on='FIO', right_on='FIO', how='left')
final_results = final_results.merge(pokazatel_working_counts, left_on='FIO', right_on='FIO', how='left')
final_results = final_results.merge(meropriatiy_counts, left_on='FIO', right_on='FIO', how='left')
final_results = final_results.merge(meropriatiy_working_counts, left_on='FIO', right_on='FIO', how='left')

# Замена NaN на 0
final_results.fillna(0, inplace=True)

# Создание строки с суммами
sum_row = final_results.sum(numeric_only=True)
sum_row['FIO'] = 'Сумма'

# Добавление строки с суммами в DataFrame
final_results = pd.concat([pd.DataFrame([sum_row]), final_results], ignore_index=True)

cols = final_results.columns.tolist()
cols = ['FIO'] + [col for col in cols if col != 'FIO']
final_results = final_results[cols]

# Запись результатов в один лист Excel
final_results.to_excel('C:\\Users\\aibul\\work\\count_risks\\risks\\output\\results.xlsx', index=False)

print("Результаты успешно выгружены в файл 'results.xlsx'")
