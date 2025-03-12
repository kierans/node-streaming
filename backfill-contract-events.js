/* eslint-disable @typescript-eslint/no-var-requires, no-plusplus, no-restricted-syntax, no-shadow, no-use-before-define, no-console */
/*
 * There are a number of env vars which control the behaviour of the script
 * - BATCH_SIZE (the number of accounts to process in a batch; defaults to 10)
 */
const fs = require('node:fs')
const path = require('node:path')
const { Transform } = require('node:stream')
const { pipeline } = require('node:stream/promises')

const batchSize = parseInt(process.env.BATCH_SIZE, 10) || 10

/*
 * A Transform is a Duplex stream, in that it is Readable and Writable. When data is being written to it, when the
 * write buffer is full, `write` will return false, and the client needs to wait for the `drain` event before writing
 * more data.
 *
 * When adding data to the Readable read buffer, the `push` method is used. However, if the consumer of the Readable
 * is slower than the transform is pushing data (for example printing to stdout), `push` will return false. How is
 * the `transform` method to know when it can continue to push data? Turns out there isn't an event for that.
 *
 * By reading the docs on push[1] and _read[2] we can see that when a consumer is wanting to read more data from the
 * Readable, `_read` will be called. Therefore, we can emulate what the `drain` event does by emitting an event when
 * the caller is ready for more data.
 *
 * Subclasses will need to wait for the `ready` event if `push` returns false.
 *
 * [1] - https://nodejs.org/docs/latest-v18.x/api/stream.html#readablepushchunk-encoding
 * [2] - https://nodejs.org/docs/latest-v18.x/api/stream.html#readable_readsize
 */
class ReadySignalingTransform extends Transform {
  constructor(opts) {
    super(opts)
  }

  _read(size) {
    super._read(size)

    this.emit("ready")
  }
}

if (process.argv.length < 5) {
  console.error(`Usage: ${process.argv[1]} <in> <sql> <log>`)
  process.exit(1)
}

main(process.argv.slice(2)).catch(console.error)

async function main(args) {
  const now = Date.now()
  const input = readFrom(args[0])
  const out = writeTo(args[1])
  const logger = createLogger(writeTo(args[2]))

  await backfillEvents(input, out, logger)

  const time = Date.now() - now
  logger(`Took ~${Math.ceil(time / 1000)} seconds`)
}

async function backfillEvents(input, out, logger) {
  await pipeline(
    input,
    createAccountNumberParserStream(logger),
    createAccountNumberBufferStream(logger),
    createBackfillStream(logger),
    out
  )
}

function createAccountNumberParserStream() {
  let rest = ''

  // toLines :: String -> [ [String], String ]
  const toLines = (str) => {
    const lines = str.split("\n")

    return [ lines.slice(0, -1), lines.slice(lines.length - 1).join('') ]
  }

  return new ReadySignalingTransform({
    allowHalfOpen: true,
    readableHighWaterMark: 1,
    readableObjectMode: true,
    writableHighWaterMark: batchSize,
    writableObjectMode: false,
    transform(chunk, encoding, callback) {
      const [ lines, rem ] = toLines(rest + chunk.toString())
      const more = this.push(lines)

      rest = rem

      if (!more) {
        this.once('ready', callback)
      }
      else {
        callback()
      }
    },
    flush(callback) {
      if (rest.length > 0) {
        const [ lines, rem ] = toLines(rest)

        if (rem !== '') {
          throw new Error('Left over data')
        }

        this.push(lines)
      }

      this.push(null)

      callback()
    },
  })
}

function createAccountNumberBufferStream() {
  let numbers = []

  function pushAccountNumbersInBatches(numbers, callback) {
    let more = true

    while (numbers.length >= batchSize && more) {
      more = this.push(numbers.slice(0, batchSize));
      numbers = numbers.slice(batchSize)
    }

    if (!more) {
      this.once('ready', () => pushAccountNumbersInBatches.call(this, numbers, callback))
    }
    else {
      callback(numbers)
    }
  }

  return new ReadySignalingTransform({
    allowHalfOpen: true,
    highWaterMark: 1,
    objectMode: true,
    transform(chunk, encoding, callback) {
      numbers = numbers.concat(chunk)

      pushAccountNumbersInBatches.call(this, numbers, (rest) => {
        numbers = rest
        callback()
      })
    },
    flush(callback) {
      if (numbers.length > 0) {
        this.push(numbers)
      }

      this.push(null)

      callback()
    },
  })
}

function createBackfillStream(logger) {
  function printSql(statements, callback) {
    let more = true

    for (let i = 0; i < statements.length; i++) {
      const line = statements[i]
      more = this.push(`${line}\n`);

      if (!more) {
        this.once('ready', () => {
          printSql.call(this, statements.slice(i+1), callback)
        })

        return
      }
    }

    if (more) {
      callback();
    }
  }

  return new ReadySignalingTransform({
    allowHalfOpen: true,
    readableObjectMode: false,
    writableObjectMode: true,
    transform(numbers, _, callback) {
      logger(`Back-filling ${numbers.length} account numbers`)

      printSql.call(this, numbers, () => callback())
    },
    flush(callback) {
      this.push(null)

      callback()
    }
  })
}

function readFrom(file) {
  return fs.createReadStream(path.resolve(__dirname, file), 'utf8')
}

function writeTo(file) {
  return fs.createWriteStream(resolve(file), 'utf8')
}

function resolve(file) {
  return path.resolve(__dirname, file)
}

function createLogger(stream) {
  return (message) => stream.write(`${message}\n`)
}
