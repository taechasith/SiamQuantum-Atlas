import { AwsClient } from 'aws4fetch'

const accountId = process.env.R2_ACCOUNT_ID
const accessKeyId = process.env.R2_ACCESS_KEY_ID
const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY
const bucket = process.env.R2_BUCKET ?? 'qatlas-storage'

if (!accountId || !accessKeyId || !secretAccessKey) {
  console.error('Missing R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, or R2_SECRET_ACCESS_KEY.')
  process.exit(1)
}

const client = new AwsClient({
  accessKeyId,
  secretAccessKey,
  service: 's3',
  region: 'auto',
})

const key = `healthchecks/r2-check-${Date.now()}.txt`
const url = `https://${accountId}.r2.cloudflarestorage.com/${bucket}/${key}`

const put = await client.fetch(url, {
  method: 'PUT',
  headers: { 'Content-Type': 'text/plain' },
  body: 'ok',
})
if (!put.ok) {
  console.error(`R2 PUT failed: ${put.status} ${await put.text()}`)
  process.exit(1)
}

const get = await client.fetch(url)
if (!get.ok || (await get.text()) !== 'ok') {
  console.error(`R2 GET failed: ${get.status}`)
  process.exit(1)
}

const remove = await client.fetch(url, { method: 'DELETE' })
if (!remove.ok && remove.status !== 404) {
  console.error(`R2 DELETE failed: ${remove.status}`)
  process.exit(1)
}

console.log(`R2 OK: ${bucket}/${key} put/get/delete succeeded.`)
