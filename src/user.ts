import { loadUser, type RequestUser } from '@ghuri/core'
import type { UserSnapshot } from './types'

/** Keep what a consumer needs from a user, small enough to live in the connection record. */
export function snapshotUser(user: RequestUser): UserSnapshot {
  const snap: UserSnapshot = {
    pk: user.isAuthenticated ? user.pk : null,
    isAuthenticated: !!user.isAuthenticated,
  }
  if (user.username) snap.username = String(user.username)
  if (user.isActive !== undefined) snap.isActive = !!user.isActive
  if (user.isStaff) snap.isStaff = true
  if (user.isSuperuser) snap.isSuperuser = true
  return snap
}

/**
 * `this.user` in a consumer: the handshake's user, rebuilt from its snapshot. Flags come from the snapshot;
 * permission checks load the real user (core `loadUser`, registered by `@ghuri/auth`) once per event.
 */
export class RealtimeUser implements RequestUser {
  readonly pk: unknown
  readonly username: string
  readonly isAuthenticated: boolean
  readonly isAnonymous: boolean
  readonly isActive: boolean
  readonly isStaff: boolean
  readonly isSuperuser: boolean
  private loaded?: Promise<RequestUser | null>

  constructor(readonly snapshot: UserSnapshot) {
    this.pk = snapshot.pk
    this.username = snapshot.username ?? ''
    this.isAuthenticated = snapshot.isAuthenticated
    this.isAnonymous = !snapshot.isAuthenticated
    this.isActive = snapshot.isActive ?? snapshot.isAuthenticated
    this.isStaff = !!snapshot.isStaff
    this.isSuperuser = !!snapshot.isSuperuser
  }

  /** The full user object (null for anonymous users or when no loader is registered). */
  load(): Promise<RequestUser | null> {
    if (!this.isAuthenticated) return Promise.resolve(null)
    this.loaded ??= loadUser(this.pk)
    return this.loaded
  }

  async hasPerm(perm: string, obj?: unknown): Promise<boolean> {
    if (!this.isAuthenticated || !this.isActive) return false
    if (this.isSuperuser) return true
    const u = await this.load()
    return u ? u.hasPerm(perm, obj) : false
  }

  async hasPerms(perms: string[], obj?: unknown): Promise<boolean> {
    for (const p of perms) if (!(await this.hasPerm(p, obj))) return false
    return true
  }

  async hasModulePerms(appLabel: string): Promise<boolean> {
    if (!this.isAuthenticated || !this.isActive) return false
    if (this.isSuperuser) return true
    const u = await this.load()
    return u?.hasModulePerms ? u.hasModulePerms(appLabel) : false
  }

  toString(): string {
    return this.isAuthenticated ? this.username || String(this.pk) : 'AnonymousUser'
  }
}
