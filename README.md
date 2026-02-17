# Inventory Removal Utility

This utility processes a product input file and removes EPCs from
the inventory for the given products
------------------------------------------------------------------------

## 🚀 Execution

Run the script using:

``` bash
poetry run python -m main -e "dev2" -b "0a27b1ed-a8f4-4821-93f5-742157310336" -sl 10 -i "./dev-testinng-product-codes.csv"
```

------------------------------------------------------------------------

## 📦 Using Poetry

This project uses **Poetry** for dependency management.

### Install dependencies:

``` bash
poetry install
```

### Run the application:

``` bash
poetry run python -m main [arguments]
```

------------------------------------------------------------------------

## 📌 Arguments

### **Required Arguments**

  Argument               Description
  ---------------------- ---------------------------------------------
  `-e`, `--env`          Environment name (e.g. `dev`, `qa`, `prod`)
  `-b`, `--buid`         Business Unit ID (UUID)
  `-i`, `--input-file`   Input CSV or Excel file

------------------------------------------------------------------------

### **Optional Arguments**

  -----------------------------------------------------------------------
  Argument                       Description
  ------------------------------ ----------------------------------------
  `-s`, `--site-ids`             Space-separated list of Site IDs (UUIDs)

  `-sl`, `--site-limit`          Number of sites to process when Site IDs
                                 are not provided

  `-d`,                          Denormalized Recipe ID (UUID)
  `--denormalized-recipe-id`     
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## ⚠️ Important Validation Rules

### ✅ **Site Selection Logic**

You must provide **either**:

✔ `--site-ids`

**OR**

✔ `--site-limit`

❌ **Do NOT provide both**

If both parameters are provided, execution will fail.

------------------------------------------------------------------------

## 📂 Input File Format

The input file must contain product details.

### Example CSV:

    product_code
    123456
    7891011

------------------------------------------------------------------------

## ✅ Example Usages

### **Using Site Limit**

``` bash
poetry run python -m main \
    -e "dev2" \
    -b "0a27b1ed-a8f4-4821-93f5-742157310336" \
    -sl 10 \
    -i "products.csv"
```

------------------------------------------------------------------------

### **Using Explicit Site IDs**

``` bash
poetry run python -m main \
    -e "dev2" \
    -b "0a27b1ed-a8f4-4821-93f5-742157310336" \
    -s "6ea26450-cc16-47cb-81e9-bb1914f4576b" \
       "aebffe02-956c-4d12-9f9c-376dcb50a965" \
    -i "products.csv"
```

------------------------------------------------------------------------

## ✅ UUID Validation

The following parameters require valid UUID values:

-   Business Unit ID (`--buid`)
-   Site IDs (`--site-ids`)
-   Denormalized Recipe ID (`--denormalized-recipe-id`)

Invalid UUIDs will cause execution failure.

------------------------------------------------------------------------

## 🛠 Requirements

-   Python 3.x
-   Poetry

------------------------------------------------------------------------
