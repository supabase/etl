import { siteConfig, withBasePath } from '@/lib/site';
import type { MetadataRoute } from 'next';

export const dynamic = 'force-static';

export default function manifest(): MetadataRoute.Manifest {
  return {
    name: siteConfig.name,
    short_name: siteConfig.shortName,
    description: siteConfig.description,
    start_url: withBasePath('/'),
    scope: withBasePath('/'),
    display: 'standalone',
    background_color: '#0c1210',
    theme_color: '#3ecf8e',
    icons: [{ src: withBasePath('/assets/etl-favicon.svg'), sizes: 'any', type: 'image/svg+xml' }],
  };
}
