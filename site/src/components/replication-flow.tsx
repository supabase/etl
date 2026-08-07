import { EtlMark } from '@/components/brand';
import {
  ChartNoAxesColumnIncreasing,
  Search,
  Warehouse,
  Waves,
  Zap,
  type LucideIcon,
} from 'lucide-react';
import type { CSSProperties, SVGProps } from 'react';

/* The Supabase design system's own `postgres` custom icon. Lucide has no
   Postgres mark, and the system says to reach for a custom icon in exactly that
   case. Rendered at the custom-icon stroke weight of 1. */
function PostgresMark(props: SVGProps<SVGSVGElement>) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...props}
    >
      <path d="M11.5466 3.23108C11.2704 3.23108 11.0466 3.45494 11.0466 3.73108C11.0466 4.00722 11.2704 4.23108 11.5466 4.23108V3.23108ZM20.6569 19.5046C20.6569 19.2285 20.433 19.0046 20.1569 19.0046C19.8808 19.0046 19.6569 19.2285 19.6569 19.5046H20.6569ZM19.6569 15.9656C19.6569 16.2417 19.8808 16.4656 20.1569 16.4656C20.433 16.4656 20.6569 16.2417 20.6569 15.9656H19.6569ZM13.0119 19.5536C12.959 19.2826 12.6964 19.1058 12.4254 19.1586C12.1544 19.2115 11.9775 19.4741 12.0304 19.7452L13.0119 19.5536ZM9.87381 18.8565L9.37381 18.8565V18.8565H9.87381ZM4.20721 5.29932L3.73784 5.12699L3.73784 5.127L4.20721 5.29932ZM2.52527 9.88046L2.0559 9.70814L2.0559 9.70814L2.52527 9.88046ZM3.24922 12.6873L2.92176 13.0651L2.92176 13.0651L3.24922 12.6873ZM4.40334 13.6875L4.07588 14.0653L4.07588 14.0653L4.40334 13.6875ZM5.00971 15.0154L4.50971 15.0154L4.50971 15.0154L5.00971 15.0154ZM5.00969 16.8536L5.50969 16.8536L5.50969 16.8536L5.00969 16.8536ZM9.87385 10.6661L9.37385 10.6661L9.37385 10.6661L9.87385 10.6661ZM11.9399 6.90783C12.136 6.71339 12.1373 6.39681 11.9429 6.20073C11.7484 6.00464 11.4319 6.00331 11.2358 6.19774L11.9399 6.90783ZM17.841 18.7506L17.8409 18.2506L17.8409 18.2506L17.841 18.7506ZM23.0676 19.2506C23.3437 19.2506 23.5676 19.0267 23.5675 18.7506C23.5675 18.4744 23.3437 18.2506 23.0675 18.2506L23.0676 19.2506ZM13.0171 15.2446C12.8955 14.9967 12.5959 14.8943 12.348 15.016C12.1001 15.1376 11.9978 15.4372 12.1194 15.6851L13.0171 15.2446ZM11.5466 4.23108H12.608V3.23108H11.5466V4.23108ZM19.6569 19.5046C19.6569 21.0264 18.2467 22.2606 16.3033 22.2606V23.2606C18.6165 23.2606 20.6569 21.7449 20.6569 19.5046H19.6569ZM12.608 4.23108C16.501 4.23108 19.6569 7.38698 19.6569 11.28H20.6569C20.6569 6.8347 17.0533 3.23108 12.608 3.23108V4.23108ZM19.6569 11.28V15.9656H20.6569V11.28H19.6569ZM16.3033 22.2606C14.6727 22.2606 13.313 21.0964 13.0119 19.5536L12.0304 19.7452C12.4214 21.7485 14.1852 23.2606 16.3033 23.2606V22.2606ZM11.6552 3.22339H6.46459V4.22339H11.6552V3.22339ZM3.73784 5.127L2.0559 9.70814L2.99464 10.0528L4.67658 5.47164L3.73784 5.127ZM2.92176 13.0651L4.07588 14.0653L4.7308 13.3096L3.57668 12.3094L2.92176 13.0651ZM4.50971 15.0154L4.50969 16.8536L5.50969 16.8536L5.50971 15.0154L4.50971 15.0154ZM10.3739 10.6661C10.3739 9.25463 10.9376 7.90166 11.9399 6.90783L11.2358 6.19774C10.0442 7.37934 9.37387 8.98795 9.37385 10.6661L10.3739 10.6661ZM2.0559 9.70814C1.61893 10.8983 1.96363 12.2348 2.92176 13.0651L3.57668 12.3094C2.9326 11.7512 2.70089 10.8529 2.99464 10.0528L2.0559 9.70814ZM8.44319 20.7872C9.50944 20.7872 10.3738 19.9228 10.3738 18.8565H9.37381C9.37381 19.3705 8.95716 19.7872 8.44319 19.7872V20.7872ZM8.44319 19.7872C6.82305 19.7872 5.50967 18.4738 5.50969 16.8536L4.50969 16.8536C4.50966 19.026 6.27075 20.7872 8.44319 20.7872V19.7872ZM4.07588 14.0653C4.35142 14.3041 4.50972 14.6508 4.50971 15.0154L5.50971 15.0154C5.50972 14.3608 5.22552 13.7384 4.7308 13.3096L4.07588 14.0653ZM6.46459 3.22339C5.24643 3.22339 4.15768 3.98347 3.73784 5.12699L4.67658 5.47164C4.95188 4.7218 5.66581 4.22339 6.46459 4.22339V3.22339ZM17.841 19.2506L23.0676 19.2506L23.0675 18.2506L17.8409 18.2506L17.841 19.2506ZM12.1194 15.6851C13.1905 17.8676 15.4098 19.2507 17.841 19.2506L17.8409 18.2506C15.7913 18.2507 13.9201 17.0846 13.0171 15.2446L12.1194 15.6851ZM10.3738 18.8565L10.3739 10.6661L9.37385 10.6661L9.37381 18.8565L10.3738 18.8565Z" />
      <path d="M15.7151 11.2056C15.7151 11.6257 16.0556 11.9663 16.4758 11.9663C16.8959 11.9663 17.2365 11.6257 17.2365 11.2056C17.2365 10.7854 16.8959 10.4448 16.4758 10.4448C16.0556 10.4448 15.7151 10.7854 15.7151 11.2056Z" />
    </svg>
  );
}

/* ETL writes to arbitrary destinations, so the node cycles through the kinds of
   system a pipeline feeds rather than naming any one vendor. */
const DESTINATIONS: { label: string; Icon: LucideIcon }[] = [
  { label: 'Analytics', Icon: ChartNoAxesColumnIncreasing },
  { label: 'Search', Icon: Search },
  { label: 'Cache', Icon: Zap },
  { label: 'Warehouse', Icon: Warehouse },
  { label: 'Data lake', Icon: Waves },
];

type PacketShape = 'circle' | 'square' | 'squircle' | 'triangle';

type Packet = {
  shape: PacketShape;
  /* Seconds into the lane's cycle at which this packet enters. */
  offset: number;
};

type Lane = {
  /* Seconds for one end-to-end traversal. */
  cycle: number;
  packets: Packet[];
};

const PACKET_SHAPES: PacketShape[] = ['circle', 'square', 'squircle', 'triangle'];

/* Both conduits render the same lanes so a packet that enters the hub is the one
   that leaves it; the stylesheet holds the outbound copy back by an inbound
   traversal plus the hub dwell. */

const LANE_COUNT = 3;
const PACKETS_PER_LANE = 3;

/* Seeded so the server and client agree; the numbers only need to look
   unpatterned, not be unpredictable. */
function createRandom(seed: number) {
  let state = seed;

  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let value = Math.imul(state ^ (state >>> 15), 1 | state);
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;

    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}

/* Packets keep a uniform size; only spacing and speed vary. Speed drifts only
   slightly between lanes, while spacing is jittered within each lane's slots by
   enough to look irregular without letting two packets converge. */
const LANE_CYCLE = 1.3;
const LANE_SPEED_SPREAD = 0.16;
const PACKET_SPACING_JITTER = 0.4;

function buildLanes(): Lane[] {
  const random = createRandom(0x5eed);

  return Array.from({ length: LANE_COUNT }, () => {
    const cycle = LANE_CYCLE + (random() - 0.5) * LANE_SPEED_SPREAD;

    return {
      cycle,
      packets: Array.from({ length: PACKETS_PER_LANE }, (_unused, slot) => ({
        shape: PACKET_SHAPES[Math.floor(random() * PACKET_SHAPES.length)],
        offset:
          ((slot + 0.5 + (random() - 0.5) * PACKET_SPACING_JITTER) / PACKETS_PER_LANE) * cycle,
      })),
    };
  });
}

const FLOW_LANES = buildLanes();

/* Held back until the housings have landed and both pipes have finished
   reaching outward. Keep in step with --etl-t-pipe in the stylesheet. */
const FLOW_START_DELAY = 2.95;

function destinationSlot(index: number): CSSProperties {
  /* Analytics is already visible before the animation starts. Begin its exit
     with Search's entrance so the first handoff does not reveal Analytics twice. */
  const cycleIndex = index === 0 ? 1 : index;

  return { '--destination-index': cycleIndex } as CSSProperties;
}

function FlowConnection({ direction }: { direction: 'source' | 'destination' }) {
  return (
    <div className={`etl-flow-conduit etl-flow-conduit-${direction}`}>
      {FLOW_LANES.map((lane, laneIndex) => (
        <span
          className="etl-flow-lane"
          key={laneIndex}
          style={
            {
              '--lane-cycle': `${lane.cycle.toFixed(3)}s`,
              '--lane-index': laneIndex,
            } as CSSProperties
          }
        >
          {lane.packets.map((packet, packetIndex) => (
            <span
              className={`etl-data-packet etl-data-packet-${packet.shape}`}
              key={packetIndex}
              style={
                {
                  '--packet-delay': `${(FLOW_START_DELAY + packet.offset).toFixed(3)}s`,
                } as CSSProperties
              }
            >
              <i />
            </span>
          ))}
        </span>
      ))}
    </div>
  );
}

export function ReplicationFlow() {
  return (
    <div className="etl-flow-scene" aria-hidden="true">
      <div className="etl-flow-grid" />
      <div className="etl-flow-glow" />

      <div className="etl-flow-stage">
        <div className="etl-flow-node etl-flow-node-source">
          <span className="etl-flow-node-kicker">Source</span>
          <span className="etl-flow-node-icon">
            <PostgresMark className="etl-postgres-mark" />
          </span>
          <span className="etl-flow-node-label">Postgres</span>
        </div>

        <FlowConnection direction="source" />

        <div className="etl-flow-node etl-flow-node-core">
          <span className="etl-flow-node-kicker">Pipeline</span>
          <span className="etl-flow-node-icon">
            <EtlMark className="etl-flow-etl-mark" />
          </span>
          <span className="etl-flow-node-label">Supabase ETL</span>
        </div>

        <FlowConnection direction="destination" />

        <div className="etl-flow-node etl-flow-node-destination">
          <span className="etl-flow-node-kicker">Destination</span>
          <div className="etl-destination-stage">
            {DESTINATIONS.map(({ label, Icon }, index) => (
              <div className="etl-destination-face" key={label} style={destinationSlot(index)}>
                <span className="etl-flow-node-icon">
                  <Icon strokeWidth={1.5} />
                </span>
                <span className="etl-flow-node-label">{label}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="etl-flow-fade" />
      <div className="etl-flow-vignette" />
    </div>
  );
}
