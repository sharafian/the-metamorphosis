module.exports = {
  promisify: (fun) => {
    return (...args) => new Promise((resolve, reject) => {
      fun(...args, (error, result) => {
        if (error) {
          reject(error)
          return
        }
        resolve(result)
      })
    })
  }
}
