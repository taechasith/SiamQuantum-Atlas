import { AwsClient } from 'aws4fetch'
import type { AppContext, R2BucketLike } from '../db/client'

export type StorageBody = Blob | ArrayBuffer | Uint8Array | ReadableStream

export interface StoredObject {
  body: ReadableStream
  contentType: string
}

export interface ObjectStorage {
  put(key: string, body: StorageBody, contentType: string): Promise<void>
  get(key: string): Promise<StoredObject | null>
  delete(key: string): Promise<void>
}

function createBindingStorage(bucket: R2BucketLike): ObjectStorage {
  return {
    async put(key, body, contentType) {
      await bucket.put(key, body, { httpMetadata: { contentType } })
    },
    async get(key) {
      const object = await bucket.get(key)
      if (!object) return null
      return {
        body: object.body,
        contentType: object.httpMetadata?.contentType ?? 'application/octet-stream',
      }
    },
    async delete(key) {
      await bucket.delete(key)
    },
  }
}

function createS3Storage(
  accountId: string,
  accessKeyId: string,
  secretAccessKey: string,
  bucket: string
): ObjectStorage {
  const client = new AwsClient({
    accessKeyId,
    secretAccessKey,
    service: 's3',
    region: 'auto',
  })

  const objectUrl = (key: string) => {
    const encoded = key
      .split('/')
      .map((segment) => encodeURIComponent(segment))
      .join('/')
    return `https://${accountId}.r2.cloudflarestorage.com/${bucket}/${encoded}`
  }

  return {
    async put(key, body, contentType) {
      const response = await client.fetch(objectUrl(key), {
        method: 'PUT',
        headers: { 'Content-Type': contentType },
        body: body as never,
      })
      if (!response.ok) {
        throw new Error(`R2 upload failed (${response.status}).`)
      }
    },
    async get(key) {
      const response = await client.fetch(objectUrl(key))
      if (response.status === 404) return null
      if (!response.ok) {
        throw new Error(`R2 download failed (${response.status}).`)
      }
      return {
        body: response.body as ReadableStream,
        contentType: response.headers.get('content-type') ?? 'application/octet-stream',
      }
    },
    async delete(key) {
      const response = await client.fetch(objectUrl(key), { method: 'DELETE' })
      if (!response.ok && response.status !== 404) {
        throw new Error(`R2 delete failed (${response.status}).`)
      }
    },
  }
}

export function getStorage(c: AppContext): ObjectStorage | null {
  const env = c.env ?? {}
  if (env.STORAGE) return createBindingStorage(env.STORAGE)

  const accountId = env.R2_ACCOUNT_ID ?? process.env.R2_ACCOUNT_ID
  const accessKeyId = env.R2_ACCESS_KEY_ID ?? process.env.R2_ACCESS_KEY_ID
  const secretAccessKey = env.R2_SECRET_ACCESS_KEY ?? process.env.R2_SECRET_ACCESS_KEY
  const bucket = env.R2_BUCKET ?? process.env.R2_BUCKET ?? 'qatlas-storage'

  if (!accountId || !accessKeyId || !secretAccessKey) return null
  return createS3Storage(accountId, accessKeyId, secretAccessKey, bucket)
}
