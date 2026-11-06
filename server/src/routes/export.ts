import { Hono } from 'hono'
import ExcelJS from 'exceljs'
import { sql } from '../db/client'

export const exportRoutes = new Hono()

exportRoutes.get('/export/xlsx', async (c) => {
  const conditions: string[] = []
  const params: any[] = []
  const year = c.req.query('year')
  const platform = c.req.query('platform')
  if (year) {
    params.push(Number.parseInt(year, 10))
    conditions.push(`s.published_year = $${params.length}`)
  }
  if (platform) {
    params.push(platform)
    conditions.push(`s.platform = $${params.length}`)
  }
  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : ''
  const rows = await sql.unsafe<
    {
      id: number
      platform: string
      url: string
      title: string | null
      published_year: number
      view_count: number | null
      like_count: number | null
      comment_count: number | null
      content_type: string | null
      production_type: string | null
      area: string | null
      engagement_level: string | null
      lat: number | null
      lng: number | null
      city: string | null
      is_cdn_resolved: number | null
    }[]
  >(
    `SELECT s.id, s.platform, s.url, s.title, s.published_year, s.view_count, s.like_count, s.comment_count,
            e.content_type, e.production_type, e.area, e.engagement_level,
            g.lat, g.lng, g.city, g.is_cdn_resolved
     FROM sources s
     LEFT JOIN entities e ON e.source_id = s.id
     LEFT JOIN geo g ON g.source_id = s.id
     ${where}
     ORDER BY s.published_year DESC, s.id DESC`,
    params
  )

  const workbook = new ExcelJS.Workbook()
  const sheet = workbook.addWorksheet('Sources')
  sheet.addRow([
    'ID',
    'Platform',
    'URL',
    'Title',
    'Year',
    'Views',
    'Likes',
    'Comments',
    'Content Type',
    'Production Type',
    'Area',
    'Engagement Level',
    'Lat',
    'Lng',
    'City',
    'CDN Resolved',
  ])
  sheet.getRow(1).font = { bold: true }
  for (const row of rows) sheet.addRow(Object.values(row))
  const buffer = await workbook.xlsx.writeBuffer()
  const suffix = [year, platform].filter(Boolean).join('_')
  return c.body(new Uint8Array(buffer), 200, {
    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'Content-Disposition': `attachment; filename="siamquantum_atlas${suffix ? `_${suffix}` : ''}.xlsx"`,
  })
})
