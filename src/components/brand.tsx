import Image from 'next/image';
import type { SVGProps } from 'react';
import icon from '../../docs/public/assets/etl-logo.png';

export function PipelinesMark(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 101 87" fill="none" aria-hidden="true" {...props}>
      <path
        d="M1.19025 12.0161H70.1371C80.16 12.0161 88.2852 20.1413 88.2852 30.1642C88.2852 40.1871 80.16 48.3122 70.1371 48.3122H19.8142C15.5073 48.3122 12.0159 51.8036 12.0159 56.1105V86.6631"
        stroke="currentColor"
        strokeWidth="24.0318"
      />
    </svg>
  );
}

export function Brand() {
  return (
    <span className="etl-brand">
      <Image src={icon} alt="" width={28} height={28} priority />
      <span>Supabase ETL</span>
    </span>
  );
}

export function HeroBrand() {
  return (
    <div className="etl-hero-brand">
      <Image src={icon} alt="" width={40} height={40} priority />
      <div>
        <strong>Supabase ETL</strong>
        <span>Open-source Postgres CDC</span>
      </div>
    </div>
  );
}
