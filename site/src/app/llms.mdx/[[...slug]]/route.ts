import { getGeneratedPageMarkdownUrl, getLLMText, source } from '@/lib/source';
import { notFound } from 'next/navigation';

export const revalidate = false;

type MarkdownRouteProps = {
  params: Promise<{ slug?: string[] }>;
};

export async function GET(_request: Request, { params }: MarkdownRouteProps) {
  const { slug } = await params;
  const page = source.getPage(slug?.slice(0, -1));
  if (!page) notFound();

  return new Response(await getLLMText(page), {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
}

export function generateStaticParams() {
  return source.getPages().map((page) => ({ slug: getGeneratedPageMarkdownUrl(page).segments }));
}
