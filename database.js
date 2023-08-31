import knex from 'knex'

const host = 'localhost'
const port = 44444
const user = process.env.USERNAME
const password = process.env.PASSWORD
const database = 'parking'


const connection = {
  user,
  password,
  database,
  charset: 'utf8',
  timezone: 'Asia/Tokyo',
  typeCast: function (field, next) {
    if (field.type === 'JSON') {
      return (JSON.parse(field.string()))
    }
    return next()
  },
  host,
  port
}

export default knex({
  client: 'mysql',
  connection
})
