"""
Database models for the YouTube Livestream Loyalty Points system.
This file defines the SQLAlchemy models that map to database tables.
"""
import uuid
from datetime import datetime
from typing import Optional, List
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()

class User(Base):
    """User model containing core user data."""
    __tablename__ = 'users'
    
    id = Column(String, primary_key=True)  # YouTube channel ID
    username = Column(String, nullable=False, index=True)  # Added index for username lookups
    points = Column(Float, default=0.0, index=True)  # Added index for points (used in sorting)
    is_eliminated = Column(Boolean, default=False, index=True)  # Added index for filtering
    elimination_reason = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)  # Added index for filtering
    
    # Relationships
    membership = relationship("Membership", uselist=False, back_populates="user", cascade="all, delete-orphan")
    inventory_items = relationship("InventoryItem", back_populates="user", cascade="all, delete-orphan")
    controlled_users = relationship("ControlRelationship", 
                                  foreign_keys="ControlRelationship.controller_id",
                                  back_populates="controller",
                                  cascade="all, delete-orphan")
    controlling_user = relationship("ControlRelationship",
                                  foreign_keys="ControlRelationship.target_id",
                                  back_populates="target",
                                  cascade="all, delete-orphan")
    immunity_against = relationship("BuyerImmunity",
                                  foreign_keys="BuyerImmunity.target_id",
                                  back_populates="target",
                                  cascade="all, delete-orphan")
    
    # Create an index for username lookups (case-insensitive)
    __table_args__ = (
        Index('idx_username_lower', username),
    )
    
    def __repr__(self) -> str:
        return f"<User(id='{self.id}', username='{self.username}', points={self.points})>"

class Membership(Base):
    """Membership status and multiplier information."""
    __tablename__ = 'memberships'
    
    user_id = Column(String, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True)
    months_subscribed = Column(Float, nullable=False, default=0)
    multiplier = Column(Integer, nullable=False, default=1)
    last_updated = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="membership")
    
    def __repr__(self) -> str:
        return f"<Membership(user_id='{self.user_id}', months={self.months_subscribed}, multiplier={self.multiplier})>"

class ControlRelationship(Base):
    """Tracks user control relationships (buyer-target)."""
    __tablename__ = 'control_relationships'
    
    controller_id = Column(String, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True)
    target_id = Column(String, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True)
    control_percent = Column(Float, nullable=False, default=40.0)
    start_time = Column(DateTime, default=datetime.utcnow)
    last_checkin = Column(DateTime, default=datetime.utcnow, index=True)  # Added index for filtering by checkin time
    
    controller = relationship("User", foreign_keys=[controller_id], back_populates="controlled_users")
    target = relationship("User", foreign_keys=[target_id], back_populates="controlling_user")
    
    __table_args__ = (
        UniqueConstraint('target_id', name='unique_target'),
        Index('idx_control_last_checkin', last_checkin),  # Added index for expired relationship queries
    )
    
    def __repr__(self) -> str:
        return f"<ControlRelationship(controller='{self.controller_id}', target='{self.target_id}', control_percent={self.control_percent})>"

class InventoryItem(Base):
    """User inventory items."""
    __tablename__ = 'inventory_items'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(String, ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)  # Added index
    item_type = Column(String, nullable=False, index=True)  # Added index
    quantity = Column(Integer, default=1)
    last_used = Column(DateTime)
    
    user = relationship("User", back_populates="inventory_items")
    
    __table_args__ = (
        UniqueConstraint('user_id', 'item_type', name='unique_user_item'),
        Index('idx_inventory_user_type', user_id, item_type),  # Composite index for common query pattern
    )
    
    def __repr__(self) -> str:
        return f"<InventoryItem(user_id='{self.user_id}', item_type='{self.item_type}', quantity={self.quantity})>"

class Transaction(Base):
    """Transaction history with improved tracking."""
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)  # Added index
    user_id = Column(String, ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)  # Added index
    points_change = Column(Float, nullable=False)
    reason = Column(String)
    source_transaction = Column(String, nullable=True)
    is_control_distribution = Column(Boolean, default=False, index=True)  # Added index
    
    user = relationship("User")
    
    __table_args__ = (
        Index('idx_transaction_user_time', user_id, timestamp),  # Composite index for user transactions by time
        Index('idx_transaction_control_dist', is_control_distribution, timestamp),  # Index for filtering control distributions
    )
    
    def __repr__(self) -> str:
        return f"<Transaction(user_id='{self.user_id}', points_change={self.points_change}, reason='{self.reason}')>"

class BuyerImmunity(Base):
    """Track buyer immunity relationships."""
    __tablename__ = 'buyer_immunity'
    
    target_id = Column(String, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True)
    buyer_id = Column(String, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True)
    granted_at = Column(DateTime, default=datetime.utcnow)
    
    target = relationship("User", foreign_keys=[target_id], back_populates="immunity_against")
    buyer = relationship("User", foreign_keys=[buyer_id])

    __table_args__ = (
        UniqueConstraint('target_id', 'buyer_id', name='unique_immunity_relationship'),
        Index('idx_immunity_target', target_id),  # Index for finding target's immunities
        Index('idx_immunity_buyer', buyer_id),   # Index for finding buyer's immunities
    )
    
    def __repr__(self) -> str:
        return f"<BuyerImmunity(target_id='{self.target_id}', buyer_id='{self.buyer_id}')>"