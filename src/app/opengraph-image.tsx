import { ImageResponse } from 'next/og';

export const alt = 'Supabase ETL — Postgres change data capture for Rust';
export const size = { width: 1200, height: 630 };
export const contentType = 'image/png';
export const dynamic = 'force-static';

export default function OpenGraphImage() {
  return new ImageResponse(
    <div
      style={{
        width: '100%',
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        padding: 80,
        color: '#f4f8f6',
        background:
          'radial-gradient(circle at 10% 0%, rgba(62,207,142,.24), transparent 40%), #0d1512',
        fontFamily: 'Inter, Arial, sans-serif',
      }}
    >
      <div style={{ color: '#3ecf8e', fontSize: 30, fontWeight: 700, marginBottom: 36 }}>SUPABASE ETL</div>
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          fontSize: 76,
          fontWeight: 700,
          lineHeight: 1.05,
          letterSpacing: -3,
        }}
      >
        <span>Postgres replication,</span>
        <span>built for Rust.</span>
      </div>
      <div style={{ color: '#a8b9b1', fontSize: 28, marginTop: 38 }}>
        Open-source change data capture from Supabase.
      </div>
    </div>,
    size,
  );
}
