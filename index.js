import knex from './database.js'
import excelJS from 'exceljs'
import fs from 'fs'

const ipoTypes = [
  { value: null, text: '未選択' },
  { value: 0, text: '未上場' },
  { value: 1, text: '東証グロース' },
  { value: 2, text: '東証スタンダード' },
  { value: 3, text: '東証プライム' },
  { value: 4, text: '札幌証券取引所' },
  { value: 5, text: 'TOKYO PRO Market' }
]

/**
 * Read and return a sheet from an Excel file.
 *
 * @param {string} filePath - Path to the Excel file.
 * @param {string} sheetName - Name of the sheet to be read.
 * @return {object} Excel sheet object.
 */
const getSheet = async (filePath, sheetName) => {
  const workbook = new excelJS.Workbook()
  await workbook.xlsx.readFile(filePath)

  const sheet = workbook.getWorksheet(sheetName)

  if (!sheet) {
    throw new Error(`Not found ${sheetName}`)
  }

  return sheet
}

/**
 * Query company information from the database.
 *
 * @param {object} nameRows - Object containing name data.
 * @param {object} telRows - Object containing telephone data.
 * @return {Promise<Company[]>} Database query result.
 */
const getCompanies = async (nameRows, telRows) => {
  return knex('real_estate_company as company')
    .whereIn('tel', Object.values(telRows))
    .orWhere(qb => {
      Object.values(nameRows).forEach(nameRow => {
        qb.orWhereLike('name', `%${nameRow}%`)
      })
    })
}

/**
 * Merge data from Excel and the database.
 *
 * @param {{[numberRow]: string}} nameRows - Object containing name data. VD:
 * @param {{[numberRow]: string}} telRows - Object containing telephone data.
 * @param {Array<Company>} companies - Array of company data from the database.
 * @return {Array} Array of merged data objects.
 */
const mergeData = (nameRows, telRows, companies) => {
  return Object.keys(telRows).map(rowNumber => {
    console.log(rowNumber)
    return {
      rowNumber,
      tel: telRows[rowNumber],
      name: nameRows[rowNumber],
      companies: companies.filter(
        company =>
          (company.tel === telRows[rowNumber] && telRows[rowNumber]) ||
          (company.name.includes(nameRows[rowNumber]) && nameRows[rowNumber])
      )
    }
  })
}

/**
 * Read data from a column in an Excel sheet.
 *
 * @param {object} sheet - Excel sheet object.
 * @param {object} column - Column configuration object.
 * @return {object} Object containing column data.
 */
const readColumnData = async (sheet, column) => {
  const dataMap = {}
  sheet.eachRow((row, rowNumber) => {
    const cell = row.getCell(column.name)
    const dataRow = cell.value
      ? column.alias === 'name'
        ? cell.value.replaceAll('株式会社', '')
        : cell.value.replaceAll('-', '')
      : ''

    dataMap[rowNumber] = dataRow
  })

  return dataMap
}

const splitNotMatchedRows = (sheet, notMatchedRows) => {
  const notMatchedCompanies = []

  notMatchedRows.forEach(notMatchedRow => {
    const row = sheet.getRow(notMatchedRow.rowNumber)
    const rowData = []
    row.eachCell({ includeEmpty: true }, cell => {
      rowData.push(cell.value)
    })
    notMatchedCompanies.push(rowData)
  })

  const newWorkbook = new excelJS.Workbook()
  const newWorksheet = newWorkbook.addWorksheet('NotMatch')

  notMatchedCompanies.forEach(rowData => {
    newWorksheet.addRow(rowData)
  })

  return newWorkbook.xlsx.writeFile('not_match.xlsx')
}

const generateUpdateSQL = async (sheet, matchedRows, toUpdateColumns) => {
  const baseSQL = `UPDATE real_estate_company`
  const excludedColumns = ['A', 'B', 'C', 'D', 'E']
  const ipoTypeCol = 'I'

  const finalSql = matchedRows.reduce((sql, matchedRow) => {
    const row = sheet.getRow(matchedRow.rowNumber)
    sql += `${baseSQL} SET `

    const newValueOfFields = []
    row.eachCell({ includeEmpty: true }, cell => {
      if (excludedColumns.includes(cell.address.charAt(0))) return
      const matchedColumn = toUpdateColumns.find(
        x => x.name === cell.address.charAt(0)
      )

      let fieldValue = cell.value?.hyperlink ? cell.value.text : cell.value
      if (matchedColumn.name === ipoTypeCol) {
        const matchedIpoType = ipoTypes.find(x => x.text === cell.value)
        fieldValue = matchedIpoType ? matchedIpoType.value : null
      }
      newValueOfFields.push({
        field: matchedColumn.alias,
        value: fieldValue
      })
    })

    /**
     * I use the WHERE condition based on name or tel instead of WHERE in (IDs) for certainty
     * (to avoid changes after data when this SQL statement has been created) (Instead, you'll need to use SET SQL_SAFE_UPDATES = 1; since there's no filtering based on the PrimaryKey.)
     * However, it may impact the performance of executing this SQL statement (when the number of them is quite large, But when I dump the data from PRD to my local and execute the SQL query, it runs without encountering any issues)
     */
    sql += ` ${newValueOfFields
      .map(x => `${x.field}=${x.value === null ? null : `'${x.value}'`}`)
      .join(', ')} WHERE name = '${matchedRow.name}' OR tel = '${
      matchedRow.tel
    }';\n`

    return sql
  }, '')

  return fs.promises.writeFile(
    'update_real_estate_company.sql',
    `
SET SQL_SAFE_UPDATES = 0;
${finalSql}
SET SQL_SAFE_UPDATES = 1;
`
  )
}

/**
 * Processing Steps:
 * I will read the provided Excel file and filter data from the columns 会社名 and 電話番号 (implemented by the readColumnData function)
 * For the 会社名 column data: I will remove the prefix 株式会社 (if present)
 * For the 電話番号 column data: I will remove the "-" character (as I've verified that the DB doesn't store this character)
 * Then, I will proceed to retrieve data for companies matching either of these two columns (implemented in the getCompanies function)
 * Next, I will merge the data from each row in the original Excel file with the newly retrieved DB data.
 * Rows with empty companies arrays -> I will separate them into a different Excel file (implemented in the splitNotMatchedRows function)
 * Remaining records -> Generate SQL to update the new values (implemented in the generateUpdateSQL function)
 */
const filePath = './data_prd.xlsx'
const sheetName = '全エリア'
const toSearchColumns = [
  {
    name: 'C',
    alias: 'name'
  },
  {
    name: 'D',
    alias: 'tel'
  }
]
const toUpdateColumns = [
  {
    name: 'F',
    alias: 'email'
  },
  {
    name: 'G',
    alias: 'main_business_sector'
  },
  {
    name: 'H',
    alias: 'other_business_sector'
  },
  {
    name: 'I',
    alias: 'ipo_type'
  },
  {
    name: 'J',
    alias: 'office_number'
  },
  {
    name: 'K',
    alias: 'office_name'
  },
  {
    name: 'L',
    alias: 'office_type_name1'
  },
  {
    name: 'M',
    alias: 'office_type_name2'
  },
  {
    name: 'N',
    alias: 'url'
  }
]

const main = async () => {
  const sheet = await getSheet(filePath, sheetName)
  const [nameRows, telRows] = await Promise.all(
    toSearchColumns.map(column => {
      return readColumnData(sheet, column)
    })
  )

  const companies = await getCompanies(nameRows, telRows)
  const mergedData = mergeData(nameRows, telRows, companies)
  const notMatchedCompanies = mergedData.filter(x => !x.companies.length)
  const matchedCompanies = mergedData.filter(x => !!x.companies.length)

  await splitNotMatchedRows(sheet, notMatchedCompanies)
  await generateUpdateSQL(sheet, matchedCompanies, toUpdateColumns)
}

main()
