def validate(data):
    if data.empty:
        raise ValueError("Датасет пустой")

    if data.isnull().sum().sum() > 0:
        print("В датасете есть пропущенные значения")

    return True
