import { EtlMark } from '@/components/brand';
import { getPageImageUrl, source } from '@/lib/source';
import { siteConfig } from '@/lib/site';
import { ImageResponse } from 'next/og';
import { notFound } from 'next/navigation';

export const revalidate = false;

type ImageRouteProps = {
  params: Promise<{ slug: string[] }>;
};

export async function GET(_request: Request, { params }: ImageRouteProps) {
  const { slug } = await params;
  const page = source.getPage(slug.slice(0, -1));
  if (!page) notFound();
  const title = page.slugs.length === 0 ? siteConfig.title : page.data.title;

  return new ImageResponse(
    <div
      style={{
        width: '100%',
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'space-between',
        padding: '68px 76px',
        color: '#f4f8f6',
        background:
          'radial-gradient(circle at 15% 0%, rgba(62,207,142,.20), transparent 38%), #0d1512',
        fontFamily: 'Inter, Arial, sans-serif',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 20, fontSize: 30, fontWeight: 700 }}>
        <EtlMark fill="#34b27b" style={{ width: 50, height: 32 }} />
        <span>Supabase ETL</span>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 22, maxWidth: 980 }}>
        <div style={{ color: '#3ecf8e', fontSize: 22, fontWeight: 700, letterSpacing: 1.2 }}>
          POSTGRES CHANGE DATA CAPTURE · RUST
        </div>
        <div style={{ fontSize: 64, fontWeight: 700, lineHeight: 1.08, letterSpacing: -2 }}>
          {title}
        </div>
        <div style={{ color: '#a8b9b1', fontSize: 28, lineHeight: 1.4 }}>{page.data.description}</div>
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between', color: '#81938a', fontSize: 20 }}>
        <span>{siteConfig.repository}</span>
        <span>supabase.github.io/etl</span>
      </div>
    </div>,
    { width: 1200, height: 630 },
  );
}

export function generateStaticParams() {
  return source.getPages().map((page) => ({ slug: getPageImageUrl(page).segments }));
}
