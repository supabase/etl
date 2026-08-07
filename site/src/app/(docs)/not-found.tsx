import { ArrowLeft } from 'lucide-react';
import Link from 'next/link';

export default function NotFound() {
  return (
    <main className="etl-not-found">
      <span className="etl-eyebrow">404 · Documentation</span>
      <h1>This page is not part of the publication.</h1>
      <p>The URL may have changed, or the document may no longer exist.</p>
      <Link className="etl-primary-action" href="/">
        <ArrowLeft size={16} strokeWidth={1.5} /> Back to Supabase ETL
      </Link>
    </main>
  );
}
