import { PrismaClient } from "@prisma/client";

const prismaClientSingleton = () => {
  return new PrismaClient({
    log: ["info"],
  });
};

declare const globalThis: {
  prismaGlobal: ReturnType<typeof prismaClientSingleton>;
} & typeof global;

const prismaDB = globalThis.prismaGlobal ?? prismaClientSingleton();

export default prismaDB;

if (process.env.NODE_ENV !== "production") globalThis.prismaGlobal = prismaDB;
