import { source } from '@/lib/source';
import { absoluteUrl } from '@/lib/site';
import type { MetadataRoute } from 'next';

export const dynamic = 'force-static';

export default function sitemap(): MetadataRoute.Sitemap {
  return source.getPages().map((page) => ({
    url: absoluteUrl(page.url),
    changeFrequency: page.slugs.length === 0 ? 'weekly' : 'monthly',
    priority: page.slugs.length === 0 ? 1 : page.slugs[0] === 'guides' ? 0.8 : 0.7,
  }));
}
